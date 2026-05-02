package waitingroom

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestJWTAdmissionTokenIssuerIssuesVerifiableToken(t *testing.T) {
	issuer := newTestJWTAdmissionTokenIssuer(t)
	issuedAt := time.Unix(1_700_000_000, 0).UTC()

	token, err := issuer.IssueAdmissionToken(AdmissionTokenClaims{
		TenantID:  "tenant-1",
		EventID:   "event-1",
		SessionID: "session-1",
		IssuedAt:  issuedAt,
		NotBefore: issuedAt,
		ExpiresAt: issuedAt.Add(15 * time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}

	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		t.Fatalf("token parts = %d, want 3", len(parts))
	}

	signingInput := parts[0] + "." + parts[1]
	signature, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		t.Fatal(err)
	}
	if !ed25519.Verify(issuer.publicKey, []byte(signingInput), signature) {
		t.Fatal("token signature did not verify")
	}

	var header jwtHeader
	decodeJWTPart(t, parts[0], &header)
	if header.Algorithm != eddsaAlgorithm {
		t.Fatalf("header alg = %q, want %q", header.Algorithm, eddsaAlgorithm)
	}
	if header.KeyID != "test-key" {
		t.Fatalf("header kid = %q, want %q", header.KeyID, "test-key")
	}

	var payload jwtAdmissionPayload
	decodeJWTPart(t, parts[1], &payload)
	if payload.Issuer != "issuer-1" {
		t.Fatalf("issuer = %q, want issuer-1", payload.Issuer)
	}
	if payload.Audience != "checkout-api" {
		t.Fatalf("audience = %q, want checkout-api", payload.Audience)
	}
	if payload.Subject != "session-1" {
		t.Fatalf("subject = %q, want session-1", payload.Subject)
	}
	if payload.TenantID != "tenant-1" || payload.EventID != "event-1" {
		t.Fatalf("tenant/event = %q/%q, want tenant-1/event-1", payload.TenantID, payload.EventID)
	}
	if payload.Scope != admissionTokenScope {
		t.Fatalf("scope = %q, want %q", payload.Scope, admissionTokenScope)
	}
	if payload.ExpiresAt-payload.IssuedAt != int64((15 * time.Minute).Seconds()) {
		t.Fatalf("ttl = %d, want 900", payload.ExpiresAt-payload.IssuedAt)
	}
}

func TestJWTAdmissionTokenIssuerJWKSet(t *testing.T) {
	issuer := newTestJWTAdmissionTokenIssuer(t)

	keySet := issuer.AdmissionTokenJWKSet()
	if len(keySet.Keys) != 1 {
		t.Fatalf("keys = %d, want 1", len(keySet.Keys))
	}

	key := keySet.Keys[0]
	if key.KeyType != okpKeyType || key.Curve != ed25519Curve || key.Alg != eddsaAlgorithm {
		t.Fatalf("unexpected key metadata: %+v", key)
	}
	if key.KeyID != "test-key" {
		t.Fatalf("kid = %q, want test-key", key.KeyID)
	}

	publicKey, err := base64.RawURLEncoding.DecodeString(key.X)
	if err != nil {
		t.Fatal(err)
	}
	if string(publicKey) != string(issuer.publicKey) {
		t.Fatal("jwk public key did not match issuer public key")
	}
}

func newTestJWTAdmissionTokenIssuer(t *testing.T) *JWTAdmissionTokenIssuer {
	t.Helper()

	seed := make([]byte, ed25519.SeedSize)
	for i := range seed {
		seed[i] = byte(i)
	}

	issuer, err := NewJWTAdmissionTokenIssuer(ed25519.NewKeyFromSeed(seed), "test-key", "issuer-1", "checkout-api")
	if err != nil {
		t.Fatal(err)
	}
	issuer.newJWTID = func() (string, error) {
		return "jti-1", nil
	}

	return issuer
}

func decodeJWTPart(t *testing.T, part string, destination any) {
	t.Helper()

	payload, err := base64.RawURLEncoding.DecodeString(part)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(payload, destination); err != nil {
		t.Fatal(err)
	}
}
