package eod_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestEod(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Eod Suite")
}
