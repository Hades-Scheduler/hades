package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConfigValidateAuth(t *testing.T) {
	assert.Error(t, HadesAPIConfig{}.validate())
	assert.Error(t, HadesAPIConfig{AuthKey: "secret"}.validate())
	assert.Error(t, HadesAPIConfig{AuthKey: "secret", AllowUnauthenticated: true}.validate())
	assert.NoError(t, HadesAPIConfig{AuthKey: "0123456789abcdef"}.validate())
	assert.NoError(t, HadesAPIConfig{AllowUnauthenticated: true}.validate())
}
