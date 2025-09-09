package engine

import (
	"errors"
	"sync"

	pb "github.com/redbco/redb-open/api/proto/integration/v1"
)

// MemoryStore provides a simple in-memory store for integrations
type MemoryStore struct {
	mu         sync.RWMutex
	byID       map[string]*pb.Integration
	byNameToID map[string]string
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{byID: map[string]*pb.Integration{}, byNameToID: map[string]string{}}
}

func (s *MemoryStore) Create(in *pb.Integration) (*pb.Integration, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if in.Id == "" {
		return nil, errors.New("id is required")
	}
	if _, exists := s.byID[in.Id]; exists {
		return nil, errors.New("integration with id already exists")
	}
	if in.Name != "" {
		if _, exists := s.byNameToID[in.Name]; exists {
			return nil, errors.New("integration with name already exists")
		}
		s.byNameToID[in.Name] = in.Id
	}
	// store pointer directly; do not copy protobuf message with embedded mutex
	s.byID[in.Id] = in
	return in, nil
}

func (s *MemoryStore) Get(id string) (*pb.Integration, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.byID[id]
	if !ok {
		return nil, errors.New("not found")
	}
	// return the stored pointer; callers must treat as read-only
	return v, nil
}

func (s *MemoryStore) Update(in *pb.Integration) (*pb.Integration, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if in.Id == "" {
		return nil, errors.New("id is required")
	}
	cur, ok := s.byID[in.Id]
	if !ok {
		return nil, errors.New("not found")
	}
	// handle name change
	if cur.Name != in.Name {
		if in.Name != "" {
			if _, exists := s.byNameToID[in.Name]; exists {
				return nil, errors.New("integration with name already exists")
			}
			if cur.Name != "" {
				delete(s.byNameToID, cur.Name)
			}
			s.byNameToID[in.Name] = in.Id
		}
	}
	s.byID[in.Id] = in
	return in, nil
}

func (s *MemoryStore) Delete(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.byID[id]
	if !ok {
		return errors.New("not found")
	}
	if cur.Name != "" {
		delete(s.byNameToID, cur.Name)
	}
	delete(s.byID, id)
	return nil
}

func (s *MemoryStore) List(filterType pb.IntegrationType) []*pb.Integration {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]*pb.Integration, 0, len(s.byID))
	for _, v := range s.byID {
		if filterType != pb.IntegrationType_INTEGRATION_TYPE_UNSPECIFIED && v.Type != filterType {
			continue
		}
		result = append(result, v)
	}
	return result
}
