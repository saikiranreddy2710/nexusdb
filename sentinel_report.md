# Sentinel Incident Report: Authentication Timing & DoS Vulnerabilities

## Vulnerability 1: Denial of Service (DoS) via Exclusive Lock Hold

**Description:** In `crates/nexus-security/src/authn/mod.rs`, the `authenticate_password` function acquires an exclusive write lock (`RwLockWriteGuard`) on the users hash map before executing the computationally expensive `pbkdf2_hmac_sha256` hashing algorithm. By default, this algorithm runs for 600,000 iterations.

```rust
fn authenticate_password(&self, username: &str, password: &str) -> AuthResult<Identity> {
    let mut users = self.users.write(); // EXCLUSIVE WRITE LOCK ACQUIRED

    let user = users.get_mut(username).ok_or(AuthError::InvalidCredentials)?;

    // ... lockout checks ...

    // EXPENSIVE COMPUTATION WHILE HOLDING WRITE LOCK
    let hash = pbkdf2_hmac_sha256(password.as_bytes(), &user.salt, self.pbkdf2_iterations);
}
```

**Impact:**

- **Severity:** CRITICAL
- Because the exclusive write lock is held during the entire 600,000-iteration hashing process, no other thread or request can authenticate, create, drop, or unlock any user in the database.
- An attacker can exploit this by sending a flood of login requests for any existing username (e.g., `"admin"`). Each request forces the server to spend significant CPU time hashing the password while blocking all other authentication attempts across the entire NexusDB instance. This results in a complete Denial of Service for the authentication subsystem.

**Root Cause:** The lock (`self.users.write()`) encompasses the entire function scope, including the synchronous, CPU-bound password hashing operation, rather than just the state mutation operations (like incrementing `failed_attempts`).

---

## Vulnerability 2: User Enumeration via Timing Side-Channel

**Description:** In the same `authenticate_password` function, the code attempts to retrieve the user record from the users map. If the user does not exist, the `ok_or(AuthError::InvalidCredentials)?` call immediately returns an error.

```rust
let mut users = self.users.write();
let user = users
    .get_mut(username)
    .ok_or(AuthError::InvalidCredentials)?; // EARLY RETURN IF USER NOT FOUND
```

**Impact:**

- **Severity:** HIGH
- If an attacker submits a login request for a **non-existent** user, the server responds in less than a millisecond.
- If the attacker submits a login request for an **existing** user, the server must compute the 600,000-iteration PBKDF2 hash, which takes significantly longer (tens to hundreds of milliseconds, depending on hardware).
- By measuring the response time of the login endpoint, an attacker can reliably and silently build a list of valid usernames (User Enumeration). This significantly reduces the search space for brute-force or credential-stuffing attacks.

**Root Cause:** The expensive cryptographic operation is conditionally executed based on the existence of the user record.

---

## Comprehensive Remediation Plan

To resolve both vulnerabilities simultaneously, the `authenticate_password` function must be refactored to:

1. Never hold a lock during cryptographic operations.
2. Execute the cryptographic operation in constant time, regardless of user existence.

### Step-by-Step Fix

1. **Initial Read Lock:** Acquire a `read()` lock on the users map instead of a `write()` lock.
2. **Data Extraction & Dummy Values:**
   - Look up the user.
   - If the user **exists**, clone their `salt`, `password_hash`, `locked` status, and `locked_until` timestamp.
   - If the user **does not exist**, generate a dummy 16-byte salt and a dummy 32-byte hash. Track internally that the user was not found.
3. **Release Lock:** Explicitly drop the `read()` lock before performing any hashing.
4. **Unconditional Hashing:** Execute `pbkdf2_hmac_sha256` using the provided password and either the real user's salt or the dummy salt. This ensures the CPU time spent hashing is identical regardless of whether the user exists, mitigating the enumeration timing attack.
5. **State Validation:**
   - If the user was not found, return `AuthError::InvalidCredentials`.
   - If the user is locked and the lockout period has not expired, return `AuthError::AccountLocked`.
6. **Constant-Time Verification:** Use the existing `constant_time_eq` function to compare the computed hash against the real user's `password_hash`.
7. **State Mutation (Reacquiring Write Lock):**
   - If the authentication failed (invalid password) OR if a locked account needs to be auto-unlocked (timeout expired) OR if a successful login needs to reset `failed_attempts` back to 0:
     - Acquire a `write()` lock on the users map.
     - Re-fetch the user (to ensure we don't overwrite concurrent changes).
     - Update `failed_attempts`, `locked`, and `locked_until` as necessary.
      - Drop the write lock immediately.

---

## Resolution Status

**Both vulnerabilities have been FIXED.** The `authenticate_password` function in `crates/nexus-security/src/authn/mod.rs` was refactored following the remediation plan above. All 40 security crate tests pass after the fix.
