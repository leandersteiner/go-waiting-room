package waitingroom

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	defaultAdmissionTokenIssuer   = "go-waiting-room"
	defaultAdmissionTokenAudience = "admission"
	admissionTokenScope           = "admission"
	jwtType                       = "JWT"
	eddsaAlgorithm                = "EdDSA"
	okpKeyType                    = "OKP"
	ed25519Curve                  = "Ed25519"
	signatureUse                  = "sig"
	jtiBytes                      = 16
)

type AdmissionToken struct {
	TenantID  string
	EventID   string
	SessionID string
	TokenID   string
	TokenType string
	Token     string
	ExpiresIn int
}

type AdmissionTokenClaims struct {
	TenantID  string
	EventID   string
	SessionID string
	Issuer    string
	Audience  string
	JWTID     string
	IssuedAt  time.Time
	NotBefore time.Time
	ExpiresAt time.Time
}

type AdmissionTokenIssuer interface {
	IssueAdmissionToken(claims AdmissionTokenClaims) (string, error)
}

type AdmissionTokenKeySetProvider interface {
	AdmissionTokenJWKSet() JWKSet
}

type JWKSet struct {
	Keys []JWK `json:"keys"`
}

type JWK struct {
	KeyType string `json:"kty"`
	Curve   string `json:"crv"`
	KeyID   string `json:"kid"`
	Use     string `json:"use,omitempty"`
	Alg     string `json:"alg,omitempty"`
	X       string `json:"x"`
}

type JWTAdmissionTokenIssuer struct {
	privateKey ed25519.PrivateKey
	publicKey  ed25519.PublicKey
	keyID      string
	issuer     string
	audience   string
	now        func() time.Time
	newJWTID   func() (string, error)
}

func NewJWTAdmissionTokenIssuer(privateKey ed25519.PrivateKey, keyID, issuer, audience string) (*JWTAdmissionTokenIssuer, error) {
	switch len(privateKey) {
	case ed25519.SeedSize:
		privateKey = ed25519.NewKeyFromSeed(privateKey)
	case ed25519.PrivateKeySize:
	default:
		return nil, fmt.Errorf("ed25519 private key must be %d-byte seed or %d-byte private key", ed25519.SeedSize, ed25519.PrivateKeySize)
	}

	publicKey, ok := privateKey.Public().(ed25519.PublicKey)
	if !ok {
		return nil, errors.New("derive ed25519 public key")
	}

	if keyID == "" {
		keyID = keyIDFromPublicKey(publicKey)
	}
	if issuer == "" {
		issuer = defaultAdmissionTokenIssuer
	}
	if audience == "" {
		audience = defaultAdmissionTokenAudience
	}

	return &JWTAdmissionTokenIssuer{
		privateKey: privateKey,
		publicKey:  publicKey,
		keyID:      keyID,
		issuer:     issuer,
		audience:   audience,
		now:        time.Now,
		newJWTID:   newJWTID,
	}, nil
}

func NewGeneratedJWTAdmissionTokenIssuer(keyID, issuer, audience string) (*JWTAdmissionTokenIssuer, error) {
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, err
	}

	return NewJWTAdmissionTokenIssuer(privateKey, keyID, issuer, audience)
}

func ParseEd25519PrivateKey(value string) (ed25519.PrivateKey, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil, errors.New("ed25519 private key is empty")
	}

	var decoded []byte
	var err error
	for _, encoding := range []*base64.Encoding{
		base64.RawStdEncoding,
		base64.StdEncoding,
		base64.RawURLEncoding,
		base64.URLEncoding,
	} {
		decoded, err = encoding.DecodeString(value)
		if err == nil {
			return decoded, nil
		}
	}

	return nil, fmt.Errorf("decode ed25519 private key: %w", err)
}

func (i *JWTAdmissionTokenIssuer) IssueAdmissionToken(claims AdmissionTokenClaims) (string, error) {
	if strings.TrimSpace(claims.TenantID) == "" {
		return "", errors.New("tenant id is required")
	}
	if strings.TrimSpace(claims.EventID) == "" {
		return "", errors.New("event id is required")
	}
	if strings.TrimSpace(claims.SessionID) == "" {
		return "", errors.New("session id is required")
	}

	now := i.now().UTC()
	if claims.IssuedAt.IsZero() {
		claims.IssuedAt = now
	}
	if claims.NotBefore.IsZero() {
		claims.NotBefore = claims.IssuedAt
	}
	if claims.ExpiresAt.IsZero() {
		return "", errors.New("expiration is required")
	}
	if claims.JWTID == "" {
		jwtID, err := i.newJWTID()
		if err != nil {
			return "", err
		}
		claims.JWTID = jwtID
	}
	if claims.Issuer == "" {
		claims.Issuer = i.issuer
	}
	if claims.Audience == "" {
		claims.Audience = i.audience
	}

	header := jwtHeader{
		Algorithm: eddsaAlgorithm,
		Type:      jwtType,
		KeyID:     i.keyID,
	}
	payload := jwtAdmissionPayload{
		Issuer:    claims.Issuer,
		Audience:  claims.Audience,
		Subject:   claims.SessionID,
		TenantID:  claims.TenantID,
		EventID:   claims.EventID,
		Scope:     admissionTokenScope,
		JWTID:     claims.JWTID,
		IssuedAt:  claims.IssuedAt.Unix(),
		NotBefore: claims.NotBefore.Unix(),
		ExpiresAt: claims.ExpiresAt.Unix(),
	}

	encodedHeader, err := encodeJWTPart(header)
	if err != nil {
		return "", err
	}
	encodedPayload, err := encodeJWTPart(payload)
	if err != nil {
		return "", err
	}

	signingInput := encodedHeader + "." + encodedPayload
	signature := ed25519.Sign(i.privateKey, []byte(signingInput))

	return signingInput + "." + base64.RawURLEncoding.EncodeToString(signature), nil
}

func (i *JWTAdmissionTokenIssuer) AdmissionTokenJWKSet() JWKSet {
	return JWKSet{
		Keys: []JWK{
			{
				KeyType: okpKeyType,
				Curve:   ed25519Curve,
				KeyID:   i.keyID,
				Use:     signatureUse,
				Alg:     eddsaAlgorithm,
				X:       base64.RawURLEncoding.EncodeToString(i.publicKey),
			},
		},
	}
}

type jwtHeader struct {
	Algorithm string `json:"alg"`
	Type      string `json:"typ"`
	KeyID     string `json:"kid"`
}

type jwtAdmissionPayload struct {
	Issuer    string `json:"iss"`
	Audience  string `json:"aud"`
	Subject   string `json:"sub"`
	TenantID  string `json:"tenant_id"`
	EventID   string `json:"event_id"`
	Scope     string `json:"scope"`
	JWTID     string `json:"jti"`
	IssuedAt  int64  `json:"iat"`
	NotBefore int64  `json:"nbf"`
	ExpiresAt int64  `json:"exp"`
}

func encodeJWTPart(value any) (string, error) {
	payload, err := json.Marshal(value)
	if err != nil {
		return "", err
	}

	return base64.RawURLEncoding.EncodeToString(payload), nil
}

func keyIDFromPublicKey(publicKey ed25519.PublicKey) string {
	hash := sha256.Sum256(publicKey)
	return base64.RawURLEncoding.EncodeToString(hash[:16])
}

func NewAdmissionTokenID() (string, error) {
	return newJWTID()
}

func newJWTID() (string, error) {
	value := make([]byte, jtiBytes)
	if _, err := rand.Read(value); err != nil {
		return "", err
	}

	return base64.RawURLEncoding.EncodeToString(value), nil
}
