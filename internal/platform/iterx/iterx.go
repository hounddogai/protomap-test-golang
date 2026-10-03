// Package iterx has the iterators the domains range over.
package iterx

import "iter"

// Each yields the elements of a slice that keep returns true for.
func Each[T any](items []T, keep func(T) bool) iter.Seq[T] {
	return func(yield func(T) bool) {
		for _, item := range items {
			if keep != nil && !keep(item) {
				continue
			}
			if !yield(item) {
				return
			}
		}
	}
}
