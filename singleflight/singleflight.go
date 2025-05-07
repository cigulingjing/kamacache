package singleflight

import (
	"sync"
)

// Represent ongoing or completed calls
type call struct {
	wg  sync.WaitGroup
	val interface{}
	err error
}

// FlightGroup manages all kinds of calls
type FlightGroup struct {
	m sync.Map // sync.Map imporve concurrency performance
}

// Multiple calls to Do() with the same key will only call @fn once.
func (g *FlightGroup) Do(key string, fn func() (interface{}, error)) (interface{}, error) {
	// Check if there is already an ongoing call for this key
	if existing, ok := g.m.Load(key); ok {
		c := existing.(*call)
		c.wg.Wait()         // Wait for the existing request to finish
		return c.val, c.err // Return the result from the ongoing call
	}

	// If no ongoing request, create a new one
	c := &call{}
	c.wg.Add(1)
	g.m.Store(key, c) // Store the call in the map

	// Execute the function and set the result
	c.val, c.err = fn()
	c.wg.Done() // Mark the request as done

	// After the request is done, clean up the map
	g.m.Delete(key)

	return c.val, c.err
}
