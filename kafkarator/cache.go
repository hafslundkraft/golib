package kafkarator

import (
	"fmt"
	"sync"

	"github.com/hamba/avro/v2"
)

type schemaKey struct {
	id      int
	subject string
}

type parsedSchemaCache struct {
	mu sync.RWMutex
	m  map[schemaKey]avro.Schema
}

func newParsedSchemaCache() *parsedSchemaCache {
	return &parsedSchemaCache{
		m: make(map[schemaKey]avro.Schema),
	}
}

func (c *parsedSchemaCache) GetOrParse(schemaID int, subject, schemaStr string) (avro.Schema, error) {
	if schemaStr == "" {
		return nil, fmt.Errorf("schema string is empty for subject %s and id %d", subject, schemaID)
	}
	key := schemaKey{id: schemaID, subject: subject}

	// read lock for concurrent access
	c.mu.RLock()
	if s, ok := c.m[key]; ok {
		c.mu.RUnlock()
		return s, nil
	}
	c.mu.RUnlock()

	// parse
	parsed, err := avro.Parse(schemaStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse schema for subject %s and id %d: %w", subject, schemaID, err)
	}

	// write lock to update cache
	c.mu.Lock()
	if s, ok := c.m[key]; ok {
		c.mu.Unlock()
		return s, nil
	}
	c.m[key] = parsed
	c.mu.Unlock()
	return parsed, nil
}
