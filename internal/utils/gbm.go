package utils

import (
	"math"
	"math/rand"
)

func GenerateGBM(s0 float64, steps int, mu float64, sigma float64) []float64 {
	prices := make([]float64, steps)
	prices[0] = s0
	dt := 1.0
	r := rand.New(rand.NewSource(42))

	for i := 1; i < steps; i++ {
		z := r.NormFloat64()
		drift := (mu - 0.5*math.Pow(sigma, 2)) * dt
		diffusion := sigma * math.Sqrt(dt) * z
		prices[i] = prices[i-1] * math.Exp(drift+diffusion)
	}
	return prices
}
