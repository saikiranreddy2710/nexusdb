//! Vector distance / similarity functions.
//!
//! All functions operate on `&[f32]` slices and return `f32`.
//! For HNSW we need *distance* (lower = more similar), so some
//! similarity metrics are converted: e.g. cosine distance = 1 - cosine_similarity.

use std::fmt;

/// Supported distance metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum DistanceMetric {
    /// L2 (Euclidean) distance — sqrt(sum((a_i - b_i)^2)).
    L2,
    /// Cosine distance — 1 - cos(a, b).  Range [0, 2].
    Cosine,
    /// Inner (dot) product distance — -dot(a, b).
    /// Negated so that higher similarity ⟹ lower distance.
    InnerProduct,
    /// Manhattan (L1) distance — sum(|a_i - b_i|).
    Manhattan,
}

impl fmt::Display for DistanceMetric {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DistanceMetric::L2 => write!(f, "l2"),
            DistanceMetric::Cosine => write!(f, "cosine"),
            DistanceMetric::InnerProduct => write!(f, "inner_product"),
            DistanceMetric::Manhattan => write!(f, "manhattan"),
        }
    }
}

impl DistanceMetric {
    /// Computes the distance between two vectors using this metric.
    ///
    /// # Panics
    /// Panics if the two slices have different lengths.
    #[inline]
    pub fn distance(&self, a: &[f32], b: &[f32]) -> f32 {
        assert_eq!(a.len(), b.len(), "vector dimension mismatch");
        match self {
            DistanceMetric::L2 => l2_distance(a, b),
            DistanceMetric::Cosine => cosine_distance(a, b),
            DistanceMetric::InnerProduct => inner_product_distance(a, b),
            DistanceMetric::Manhattan => manhattan_distance(a, b),
        }
    }
}

/// Squared L2 distance (avoids the sqrt for ranking—same ordering).
#[inline]
pub fn l2_distance_squared(a: &[f32], b: &[f32]) -> f32 {
    a.iter()
        .zip(b.iter())
        .map(|(x, y)| {
            let d = x - y;
            d * d
        })
        .sum()
}

/// L2 (Euclidean) distance.
#[inline]
pub fn l2_distance(a: &[f32], b: &[f32]) -> f32 {
    l2_distance_squared(a, b).sqrt()
}

/// Dot product of two vectors.
#[inline]
pub fn dot_product(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b.iter()).map(|(x, y)| x * y).sum()
}

/// Cosine similarity in [-1, 1].
#[inline]
pub fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    let dot: f32 = dot_product(a, b);
    let norm_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
    let norm_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
    let denom = norm_a * norm_b;
    if denom == 0.0 {
        0.0
    } else {
        dot / denom
    }
}

/// Cosine distance = 1 - cosine_similarity.  Range [0, 2].
#[inline]
pub fn cosine_distance(a: &[f32], b: &[f32]) -> f32 {
    1.0 - cosine_similarity(a, b)
}

/// Inner-product distance = -dot(a, b).
/// Negated so that higher dot-product ⟹ lower distance.
#[inline]
pub fn inner_product_distance(a: &[f32], b: &[f32]) -> f32 {
    -dot_product(a, b)
}

/// Manhattan (L1) distance.
#[inline]
pub fn manhattan_distance(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b.iter()).map(|(x, y)| (x - y).abs()).sum()
}

/// Normalizes a vector to unit length (in-place).
/// Returns `false` if the vector is zero-length.
pub fn normalize(v: &mut [f32]) -> bool {
    let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm == 0.0 {
        return false;
    }
    let inv = 1.0 / norm;
    for x in v.iter_mut() {
        *x *= inv;
    }
    true
}

// ── tests ───────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    const EPS: f32 = 1e-5;

    #[test]
    fn test_l2_distance() {
        let a = [1.0, 0.0, 0.0];
        let b = [0.0, 1.0, 0.0];
        let d = l2_distance(&a, &b);
        assert!((d - std::f32::consts::SQRT_2).abs() < EPS);
    }

    #[test]
    fn test_l2_distance_same() {
        let a = [3.0, 4.0];
        assert!(l2_distance(&a, &a) < EPS);
    }

    #[test]
    fn test_cosine_similarity_identical() {
        let a = [1.0, 2.0, 3.0];
        let sim = cosine_similarity(&a, &a);
        assert!((sim - 1.0).abs() < EPS);
    }

    #[test]
    fn test_cosine_similarity_orthogonal() {
        let a = [1.0, 0.0];
        let b = [0.0, 1.0];
        let sim = cosine_similarity(&a, &b);
        assert!(sim.abs() < EPS);
    }

    #[test]
    fn test_cosine_distance() {
        let a = [1.0, 0.0];
        let b = [0.0, 1.0];
        let d = cosine_distance(&a, &b);
        assert!((d - 1.0).abs() < EPS);
    }

    #[test]
    fn test_cosine_distance_same() {
        let a = [1.0, 2.0, 3.0];
        let d = cosine_distance(&a, &a);
        assert!(d.abs() < EPS);
    }

    #[test]
    fn test_dot_product() {
        let a = [1.0, 2.0, 3.0];
        let b = [4.0, 5.0, 6.0];
        let d = dot_product(&a, &b);
        assert!((d - 32.0).abs() < EPS);
    }

    #[test]
    fn test_inner_product_distance() {
        let a = [1.0, 2.0, 3.0];
        let b = [4.0, 5.0, 6.0];
        let d = inner_product_distance(&a, &b);
        assert!((d - (-32.0)).abs() < EPS);
    }

    #[test]
    fn test_manhattan_distance() {
        let a = [1.0, 2.0, 3.0];
        let b = [4.0, 6.0, 3.0];
        let d = manhattan_distance(&a, &b);
        assert!((d - 7.0).abs() < EPS); // |3|+|4|+|0|
    }

    #[test]
    fn test_normalize() {
        let mut v = [3.0, 4.0];
        assert!(normalize(&mut v));
        let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
        assert!((norm - 1.0).abs() < EPS);
    }

    #[test]
    fn test_normalize_zero() {
        let mut v = [0.0, 0.0];
        assert!(!normalize(&mut v));
    }

    #[test]
    fn test_distance_metric_enum() {
        let a = [1.0, 0.0];
        let b = [0.0, 1.0];

        let l2 = DistanceMetric::L2.distance(&a, &b);
        assert!((l2 - std::f32::consts::SQRT_2).abs() < EPS);

        let cos = DistanceMetric::Cosine.distance(&a, &b);
        assert!((cos - 1.0).abs() < EPS);

        let ip = DistanceMetric::InnerProduct.distance(&a, &b);
        assert!(ip.abs() < EPS); // dot = 0, distance = 0

        let man = DistanceMetric::Manhattan.distance(&a, &b);
        assert!((man - 2.0).abs() < EPS);
    }

    #[test]
    fn test_l2_distance_squared() {
        let a = [1.0, 2.0, 3.0];
        let b = [4.0, 6.0, 3.0];
        let dsq = l2_distance_squared(&a, &b);
        assert!((dsq - 25.0).abs() < EPS); // 9 + 16 + 0
    }

    #[test]
    fn test_cosine_zero_vector() {
        let a = [0.0, 0.0];
        let b = [1.0, 2.0];
        let sim = cosine_similarity(&a, &b);
        assert!(sim.abs() < EPS);
    }

    #[test]
    fn test_distance_metric_display() {
        assert_eq!(DistanceMetric::L2.to_string(), "l2");
        assert_eq!(DistanceMetric::Cosine.to_string(), "cosine");
        assert_eq!(DistanceMetric::InnerProduct.to_string(), "inner_product");
        assert_eq!(DistanceMetric::Manhattan.to_string(), "manhattan");
    }
}
