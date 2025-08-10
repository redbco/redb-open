package engine

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/types/known/structpb"

	pb "github.com/redbco/redb-open/api/proto/integration/v1"
)

func (e *Engine) insertIntegration(ctx context.Context, in *pb.Integration) (*pb.Integration, error) {
	if e.db == nil {
		return nil, fmt.Errorf("database not initialized")
	}
	// Convert struct fields to JSONB-compatible values
	var cfg any = map[string]any{}
	var meta any = map[string]any{}
	if in.Config != nil {
		cfg = in.Config.AsMap()
	}
	if in.Metadata != nil {
		meta = in.Metadata.AsMap()
	}

	sql := `INSERT INTO integrations (
        integration_id, tenant_id, integration_name, integration_description,
        integration_type, integration_config, credential_key, integration_metadata, supported_operations,
        status
    ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`

	tenantID := ""
	if in.Metadata != nil {
		if v, ok := in.Metadata.AsMap()["tenant_id"].(string); ok {
			tenantID = v
		}
	}
	if tenantID == "" {
		return nil, fmt.Errorf("tenant_id is required in metadata")
	}
	_, err := e.db.Pool().Exec(ctx, sql,
		in.Id, tenantID, in.Name, in.Description,
		in.Type.String(), cfg, in.CredentialKey, meta, in.SupportedOperations, "STATUS_CREATED",
	)
	if err != nil {
		return nil, err
	}
	return in, nil
}

func (e *Engine) selectIntegration(ctx context.Context, id string) (*pb.Integration, error) {
	if e.db == nil {
		return nil, fmt.Errorf("database not initialized")
	}

	row := e.db.Pool().QueryRow(ctx, `SELECT integration_id, integration_name, integration_description,
        integration_type, integration_config, credential_key, integration_metadata, supported_operations
        FROM integrations WHERE integration_id=$1`, id)

	var (
		integrationID   string
		name            string
		description     string
		integrationType string
		configMap       map[string]any
		credentialKey   string
		metadataMap     map[string]any
		supportedOps    []string
	)

	if err := row.Scan(&integrationID, &name, &description, &integrationType, &configMap, &credentialKey, &metadataMap, &supportedOps); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("not found")
		}
		return nil, err
	}

	cfgStruct, _ := structpb.NewStruct(configMap)
	metaStruct, _ := structpb.NewStruct(metadataMap)

	out := &pb.Integration{
		Id:                  integrationID,
		Name:                name,
		Description:         description,
		Type:                pb.IntegrationType(pb.IntegrationType_value["INTEGRATION_TYPE_"+integrationType]),
		Config:              cfgStruct,
		CredentialKey:       credentialKey,
		Metadata:            metaStruct,
		SupportedOperations: supportedOps,
	}
	return out, nil
}
