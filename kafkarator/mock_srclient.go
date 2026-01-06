package kafkarator

import (
	"errors"

	sr "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
)

// Unified mock for BOTH serializer and deserializer tests
type mockSRClient struct {
	// Map subject -> metadata
	latest map[string]sr.SchemaMetadata

	// Map (subject, id) -> SchemaInfo
	byID map[string]map[int]sr.SchemaInfo

	errLatest error
	errByID   error

	getLatestCalls         int
	getBySubjectAndIDCalls int
}

func newMockSRClient() *mockSRClient {
	return &mockSRClient{
		latest: map[string]sr.SchemaMetadata{},
		byID:   map[string]map[int]sr.SchemaInfo{},
	}
}

// Methods used by AvroSerializer

func (m *mockSRClient) GetLatestSchemaMetadata(subject string) (sr.SchemaMetadata, error) {
	m.getLatestCalls++
	if m.errLatest != nil {
		return sr.SchemaMetadata{}, m.errLatest
	}
	md, ok := m.latest[subject]
	if !ok {
		return sr.SchemaMetadata{}, errors.New("not found")
	}
	return md, nil
}

// Methods used by AvroDeserializer

func (m *mockSRClient) GetBySubjectAndID(subject string, id int) (sr.SchemaInfo, error) {
	m.getBySubjectAndIDCalls++

	if m.errByID != nil {
		return sr.SchemaInfo{}, m.errByID
	}
	subjectSchemas, ok := m.byID[subject]
	if !ok {
		return sr.SchemaInfo{}, errors.New("subject not found")
	}
	info, ok := subjectSchemas[id]
	if !ok {
		return sr.SchemaInfo{}, errors.New("id not found")
	}
	return info, nil
}

// Stubs out other methods not used by client
func (m *mockSRClient) Config() *sr.Config                                { return nil }
func (m *mockSRClient) GetAllContexts() ([]string, error)                 { return nil, nil }
func (m *mockSRClient) Register(string, sr.SchemaInfo, bool) (int, error) { return 0, nil }
func (m *mockSRClient) RegisterFullResponse(string, sr.SchemaInfo, bool) (sr.SchemaMetadata, error) {
	return sr.SchemaMetadata{}, nil
}
func (m *mockSRClient) GetByGUID(string) (sr.SchemaInfo, error) { return sr.SchemaInfo{}, nil }
func (m *mockSRClient) GetSubjectsAndVersionsByID(int) ([]sr.SubjectAndVersion, error) {
	return nil, nil
}
func (m *mockSRClient) GetID(string, sr.SchemaInfo, bool) (int, error) { return 0, nil }
func (m *mockSRClient) GetIDFullResponse(string, sr.SchemaInfo, bool) (sr.SchemaMetadata, error) {
	return sr.SchemaMetadata{}, nil
}

func (m *mockSRClient) GetSchemaMetadata(string, int) (sr.SchemaMetadata, error) {
	return sr.SchemaMetadata{}, nil
}

func (m *mockSRClient) GetSchemaMetadataIncludeDeleted(string, int, bool) (sr.SchemaMetadata, error) {
	return sr.SchemaMetadata{}, nil
}

func (m *mockSRClient) GetLatestWithMetadata(string, map[string]string, bool) (sr.SchemaMetadata, error) {
	return sr.SchemaMetadata{}, nil
}
func (m *mockSRClient) GetAllVersions(string) ([]int, error)                { return nil, nil }
func (m *mockSRClient) GetVersion(string, sr.SchemaInfo, bool) (int, error) { return 0, nil }
func (m *mockSRClient) GetVersionIncludeDeleted(string, sr.SchemaInfo, bool, bool) (int, error) {
	return 0, nil
}
func (m *mockSRClient) GetAllSubjects() ([]string, error)                   { return nil, nil }
func (m *mockSRClient) DeleteSubject(string, bool) ([]int, error)           { return nil, nil }
func (m *mockSRClient) DeleteSubjectVersion(string, int, bool) (int, error) { return 0, nil }
func (m *mockSRClient) TestSubjectCompatibility(string, sr.SchemaInfo) (bool, error) {
	return false, nil
}

func (m *mockSRClient) TestCompatibility(string, int, sr.SchemaInfo) (bool, error) {
	return false, nil
}
func (m *mockSRClient) GetCompatibility(string) (sr.Compatibility, error) { return 0, nil }
func (m *mockSRClient) UpdateCompatibility(string, sr.Compatibility) (sr.Compatibility, error) {
	return 0, nil
}
func (m *mockSRClient) GetDefaultCompatibility() (sr.Compatibility, error) { return 0, nil }
func (m *mockSRClient) UpdateDefaultCompatibility(sr.Compatibility) (sr.Compatibility, error) {
	return 0, nil
}

func (m *mockSRClient) GetConfig(string, bool) (sr.ServerConfig, error) {
	return sr.ServerConfig{}, nil
}

func (m *mockSRClient) UpdateConfig(string, sr.ServerConfig) (sr.ServerConfig, error) {
	return sr.ServerConfig{}, nil
}
func (m *mockSRClient) GetDefaultConfig() (sr.ServerConfig, error) { return sr.ServerConfig{}, nil }
func (m *mockSRClient) UpdateDefaultConfig(sr.ServerConfig) (sr.ServerConfig, error) {
	return sr.ServerConfig{}, nil
}
func (m *mockSRClient) ClearLatestCaches() error { return nil }
func (m *mockSRClient) ClearCaches() error       { return nil }
func (m *mockSRClient) Close() error             { return nil }
