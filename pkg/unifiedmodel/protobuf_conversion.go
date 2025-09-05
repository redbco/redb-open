package unifiedmodel

import (
	pb "github.com/redbco/redb-open/api/proto/unifiedmodel/v1"
	"github.com/redbco/redb-open/pkg/dbcapabilities"
)

// ConvertToProto converts a Go UnifiedModel to protobuf UnifiedModel
func (um *UnifiedModel) ConvertToProto() *pb.UnifiedModel {
	if um == nil {
		return nil
	}

	pbUM := &pb.UnifiedModel{
		DatabaseType: string(um.DatabaseType),
		Tables:       make(map[string]*pb.Table),
		Schemas:      make(map[string]*pb.Schema),
		Views:        make(map[string]*pb.View),
		Functions:    make(map[string]*pb.Function),
		Procedures:   make(map[string]*pb.Procedure),
		Triggers:     make(map[string]*pb.Trigger),
		Sequences:    make(map[string]*pb.Sequence),
		Types:        make(map[string]*pb.Type),
		Indexes:      make(map[string]*pb.Index),
		Constraints:  make(map[string]*pb.Constraint),
	}

	// Convert tables
	for name, table := range um.Tables {
		pbTable := &pb.Table{
			Name:        table.Name,
			Owner:       table.Owner,
			Comment:     table.Comment,
			Labels:      table.Labels,
			Columns:     make(map[string]*pb.Column),
			Indexes:     make(map[string]*pb.Index),
			Constraints: make(map[string]*pb.Constraint),
		}

		// Convert columns
		for colName, col := range table.Columns {
			pbTable.Columns[colName] = &pb.Column{
				Name:                col.Name,
				DataType:            col.DataType,
				Nullable:            col.Nullable,
				DefaultValue:        col.Default,
				GeneratedExpression: col.GeneratedExpression,
				IsPrimaryKey:        col.IsPrimaryKey,
				IsPartitionKey:      col.IsPartitionKey,
				IsClusteringKey:     col.IsClusteringKey,
				AutoIncrement:       col.AutoIncrement,
				Collation:           col.Collation,
			}
		}

		// Convert indexes
		for idxName, idx := range table.Indexes {
			pbTable.Indexes[idxName] = &pb.Index{
				Name:       idx.Name,
				Type:       string(idx.Type),
				Columns:    idx.Columns,
				Fields:     idx.Fields,
				Expression: idx.Expression,
				Predicate:  idx.Predicate,
				Unique:     idx.Unique,
			}
		}

		// Convert constraints
		for constName, constraint := range table.Constraints {
			pbConstraint := &pb.Constraint{
				Name:       constraint.Name,
				Type:       string(constraint.Type),
				Columns:    constraint.Columns,
				Expression: constraint.Expression,
			}

			if constraint.Reference.Table != "" {
				pbConstraint.Reference = &pb.Reference{
					Table:    constraint.Reference.Table,
					Columns:  constraint.Reference.Columns,
					OnUpdate: constraint.Reference.OnUpdate,
					OnDelete: constraint.Reference.OnDelete,
				}
			}

			pbTable.Constraints[constName] = pbConstraint
		}

		pbUM.Tables[name] = pbTable
	}

	// Convert schemas
	for name, schema := range um.Schemas {
		pbUM.Schemas[name] = &pb.Schema{
			Name:    schema.Name,
			Owner:   schema.Owner,
			Comment: schema.Comment,
			Labels:  schema.Labels,
		}
	}

	// Convert views
	for name, view := range um.Views {
		pbView := &pb.View{
			Name:       view.Name,
			Definition: view.Definition,
			Comment:    view.Comment,
			Columns:    make(map[string]*pb.Column),
		}

		for colName, col := range view.Columns {
			pbView.Columns[colName] = &pb.Column{
				Name:                col.Name,
				DataType:            col.DataType,
				Nullable:            col.Nullable,
				DefaultValue:        col.Default,
				GeneratedExpression: col.GeneratedExpression,
				IsPrimaryKey:        col.IsPrimaryKey,
				IsPartitionKey:      col.IsPartitionKey,
				IsClusteringKey:     col.IsClusteringKey,
				AutoIncrement:       col.AutoIncrement,
				Collation:           col.Collation,
			}
		}

		pbUM.Views[name] = pbView
	}

	// Convert functions
	for name, function := range um.Functions {
		pbFunction := &pb.Function{
			Name:       function.Name,
			Language:   function.Language,
			Returns:    function.Returns,
			Definition: function.Definition,
			Arguments:  make([]*pb.Argument, len(function.Arguments)),
		}

		for i, arg := range function.Arguments {
			pbFunction.Arguments[i] = &pb.Argument{
				Name: arg.Name,
				Type: arg.Type,
			}
		}

		pbUM.Functions[name] = pbFunction
	}

	// Convert procedures
	for name, procedure := range um.Procedures {
		pbProcedure := &pb.Procedure{
			Name:       procedure.Name,
			Language:   procedure.Language,
			Definition: procedure.Definition,
			Arguments:  make([]*pb.Argument, len(procedure.Arguments)),
		}

		for i, arg := range procedure.Arguments {
			pbProcedure.Arguments[i] = &pb.Argument{
				Name: arg.Name,
				Type: arg.Type,
			}
		}

		pbUM.Procedures[name] = pbProcedure
	}

	// Convert triggers
	for name, trigger := range um.Triggers {
		pbUM.Triggers[name] = &pb.Trigger{
			Name:      trigger.Name,
			Table:     trigger.Table,
			Timing:    trigger.Timing,
			Events:    trigger.Events,
			Procedure: trigger.Procedure,
		}
	}

	// Convert sequences
	for name, sequence := range um.Sequences {
		pbSequence := &pb.Sequence{
			Name:      sequence.Name,
			Start:     sequence.Start,
			Increment: sequence.Increment,
			Cycle:     sequence.Cycle,
		}

		if sequence.Min != nil {
			pbSequence.MinValue = *sequence.Min
		}
		if sequence.Max != nil {
			pbSequence.MaxValue = *sequence.Max
		}
		if sequence.Cache != nil {
			pbSequence.Cache = *sequence.Cache
		}

		pbUM.Sequences[name] = pbSequence
	}

	// Convert types
	for name, umType := range um.Types {
		pbUM.Types[name] = &pb.Type{
			Name:     umType.Name,
			Category: umType.Category,
		}
	}

	return pbUM
}

// ConvertFromProto converts a protobuf UnifiedModel to Go UnifiedModel
func ConvertFromProto(pbUM *pb.UnifiedModel) *UnifiedModel {
	if pbUM == nil {
		return nil
	}

	um := &UnifiedModel{
		DatabaseType: dbcapabilities.DatabaseType(pbUM.DatabaseType),
		Tables:       make(map[string]Table),
		Schemas:      make(map[string]Schema),
		Views:        make(map[string]View),
		Functions:    make(map[string]Function),
		Procedures:   make(map[string]Procedure),
		Triggers:     make(map[string]Trigger),
		Sequences:    make(map[string]Sequence),
		Types:        make(map[string]Type),
	}

	// Convert tables
	for name, pbTable := range pbUM.Tables {
		table := Table{
			Name:        pbTable.Name,
			Owner:       pbTable.Owner,
			Comment:     pbTable.Comment,
			Labels:      pbTable.Labels,
			Columns:     make(map[string]Column),
			Indexes:     make(map[string]Index),
			Constraints: make(map[string]Constraint),
		}

		// Convert columns
		for colName, pbCol := range pbTable.Columns {
			table.Columns[colName] = Column{
				Name:                pbCol.Name,
				DataType:            pbCol.DataType,
				Nullable:            pbCol.Nullable,
				Default:             pbCol.DefaultValue,
				GeneratedExpression: pbCol.GeneratedExpression,
				IsPrimaryKey:        pbCol.IsPrimaryKey,
				IsPartitionKey:      pbCol.IsPartitionKey,
				IsClusteringKey:     pbCol.IsClusteringKey,
				AutoIncrement:       pbCol.AutoIncrement,
				Collation:           pbCol.Collation,
			}
		}

		// Convert indexes
		for idxName, pbIdx := range pbTable.Indexes {
			table.Indexes[idxName] = Index{
				Name:       pbIdx.Name,
				Type:       IndexType(pbIdx.Type),
				Columns:    pbIdx.Columns,
				Fields:     pbIdx.Fields,
				Expression: pbIdx.Expression,
				Predicate:  pbIdx.Predicate,
				Unique:     pbIdx.Unique,
			}
		}

		// Convert constraints
		for constName, pbConstraint := range pbTable.Constraints {
			constraint := Constraint{
				Name:       pbConstraint.Name,
				Type:       ConstraintType(pbConstraint.Type),
				Columns:    pbConstraint.Columns,
				Expression: pbConstraint.Expression,
			}

			if pbConstraint.Reference != nil {
				constraint.Reference = Reference{
					Table:    pbConstraint.Reference.Table,
					Columns:  pbConstraint.Reference.Columns,
					OnUpdate: pbConstraint.Reference.OnUpdate,
					OnDelete: pbConstraint.Reference.OnDelete,
				}
			}

			table.Constraints[constName] = constraint
		}

		um.Tables[name] = table
	}

	// Convert schemas
	for name, pbSchema := range pbUM.Schemas {
		um.Schemas[name] = Schema{
			Name:    pbSchema.Name,
			Owner:   pbSchema.Owner,
			Comment: pbSchema.Comment,
			Labels:  pbSchema.Labels,
		}
	}

	// Convert views
	for name, pbView := range pbUM.Views {
		view := View{
			Name:       pbView.Name,
			Definition: pbView.Definition,
			Comment:    pbView.Comment,
			Columns:    make(map[string]Column),
		}

		for colName, pbCol := range pbView.Columns {
			view.Columns[colName] = Column{
				Name:                pbCol.Name,
				DataType:            pbCol.DataType,
				Nullable:            pbCol.Nullable,
				Default:             pbCol.DefaultValue,
				GeneratedExpression: pbCol.GeneratedExpression,
				IsPrimaryKey:        pbCol.IsPrimaryKey,
				IsPartitionKey:      pbCol.IsPartitionKey,
				IsClusteringKey:     pbCol.IsClusteringKey,
				AutoIncrement:       pbCol.AutoIncrement,
				Collation:           pbCol.Collation,
			}
		}

		um.Views[name] = view
	}

	// Convert functions
	for name, pbFunction := range pbUM.Functions {
		function := Function{
			Name:       pbFunction.Name,
			Language:   pbFunction.Language,
			Returns:    pbFunction.Returns,
			Definition: pbFunction.Definition,
			Arguments:  make([]Argument, len(pbFunction.Arguments)),
		}

		for i, pbArg := range pbFunction.Arguments {
			function.Arguments[i] = Argument{
				Name: pbArg.Name,
				Type: pbArg.Type,
			}
		}

		um.Functions[name] = function
	}

	// Convert procedures
	for name, pbProcedure := range pbUM.Procedures {
		procedure := Procedure{
			Name:       pbProcedure.Name,
			Language:   pbProcedure.Language,
			Definition: pbProcedure.Definition,
			Arguments:  make([]Argument, len(pbProcedure.Arguments)),
		}

		for i, pbArg := range pbProcedure.Arguments {
			procedure.Arguments[i] = Argument{
				Name: pbArg.Name,
				Type: pbArg.Type,
			}
		}

		um.Procedures[name] = procedure
	}

	// Convert triggers
	for name, pbTrigger := range pbUM.Triggers {
		um.Triggers[name] = Trigger{
			Name:      pbTrigger.Name,
			Table:     pbTrigger.Table,
			Timing:    pbTrigger.Timing,
			Events:    pbTrigger.Events,
			Procedure: pbTrigger.Procedure,
		}
	}

	// Convert sequences
	for name, pbSequence := range pbUM.Sequences {
		sequence := Sequence{
			Name:      pbSequence.Name,
			Start:     pbSequence.Start,
			Increment: pbSequence.Increment,
			Cycle:     pbSequence.Cycle,
		}

		if pbSequence.MinValue != 0 {
			sequence.Min = &pbSequence.MinValue
		}
		if pbSequence.MaxValue != 0 {
			sequence.Max = &pbSequence.MaxValue
		}
		if pbSequence.Cache != 0 {
			sequence.Cache = &pbSequence.Cache
		}

		um.Sequences[name] = sequence
	}

	// Convert types
	for name, pbType := range pbUM.Types {
		um.Types[name] = Type{
			Name:     pbType.Name,
			Category: pbType.Category,
		}
	}

	return um
}

// ToProto is a convenience method that calls ConvertToProto
func (um *UnifiedModel) ToProto() *pb.UnifiedModel {
	return um.ConvertToProto()
}

// FromProto is a convenience function that calls ConvertFromProto
func FromProto(pbUM *pb.UnifiedModel) *UnifiedModel {
	return ConvertFromProto(pbUM)
}
