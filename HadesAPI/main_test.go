package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConfigValidateAuth(t *testing.T) {
	assert.Error(t, HadesAPIConfig{}.validate())
	assert.NoError(t, HadesAPIConfig{AuthKey: "secret"}.validate())
	assert.NoError(t, HadesAPIConfig{AllowUnauthenticated: true}.validate())
}
