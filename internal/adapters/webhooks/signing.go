package webhooks

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
)

func SignHMACSHA256(secret []byte, payload []byte) string {
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write(payload)
	return hex.EncodeToString(mac.Sum(nil))
}

func VerifyHMACSHA256(secret []byte, payload []byte, sigHex string) bool {
	expected := SignHMACSHA256(secret, payload)
	// constant-time compare
	return hmac.Equal([]byte(expected), []byte(sigHex))
}
