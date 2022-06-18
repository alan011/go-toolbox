package tools

import (
	"math/rand"
	"time"
)

func GenUUID(length int, noUpperCase bool) string {
	seed := "1234567890"
	seed += "abcdefghijklmnopqrstuvwxyz"
	if !noUpperCase {
		seed += "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
	}

	rand.Seed(time.Now().UnixNano())
	res := ""
	for i := 0; i < length; i++ {
		res += string(seed[rand.Intn(len(seed))])
	}
	return res
}
