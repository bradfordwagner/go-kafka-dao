package template

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Options", func() {
	Context("WorkerRoutines", func() {
		It("Default Worker Routines", func() {
			c := newDefaultConfig()
			Expect(c.numWorkers).To(Equal(15))
		})
		It("Overrides Worker Routines", func() {
			c := newDefaultConfig()
			numRoutines := 5
			OptionWorkerRoutines(numRoutines)(c)
			Expect(c.numWorkers).To(Equal(numRoutines))
		})
	})
})
