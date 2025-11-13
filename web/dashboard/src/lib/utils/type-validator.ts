import type { SchemaColumn } from '@/lib/api/types';

export type ValidationLevel = 'success' | 'warning' | 'error' | 'info';

export interface ValidationResult {
  isValid: boolean;
  level: ValidationLevel;
  message: string;
  details?: {
    sourceType: string;
    targetType: string;
    transformationNeeded: boolean;
    potentialDataLoss: boolean;
    issues: string[];
  };
}

/**
 * Type categories for easier comparison
 */
enum TypeCategory {
  STRING = 'string',
  INTEGER = 'integer',
  FLOAT = 'float',
  BOOLEAN = 'boolean',
  DATE = 'date',
  DATETIME = 'datetime',
  TIME = 'time',
  BINARY = 'binary',
  JSON = 'json',
  UUID = 'uuid',
  UNKNOWN = 'unknown',
}

/**
 * Maps database types to type categories
 */
function getTypeCategory(dataType: string): TypeCategory {
  const type = dataType.toLowerCase().replace(/\(.*\)/, ''); // Remove length/precision

  // String types
  if (['varchar', 'char', 'text', 'string', 'nvarchar', 'nchar', 'clob', 'longtext', 'mediumtext', 'tinytext'].includes(type)) {
    return TypeCategory.STRING;
  }

  // Integer types
  if (['int', 'integer', 'bigint', 'smallint', 'tinyint', 'mediumint', 'serial', 'bigserial'].includes(type)) {
    return TypeCategory.INTEGER;
  }

  // Float types
  if (['float', 'double', 'decimal', 'numeric', 'real', 'money', 'number'].includes(type)) {
    return TypeCategory.FLOAT;
  }

  // Boolean types
  if (['boolean', 'bool', 'bit'].includes(type)) {
    return TypeCategory.BOOLEAN;
  }

  // Date types
  if (['date'].includes(type)) {
    return TypeCategory.DATE;
  }

  // DateTime types
  if (['datetime', 'timestamp', 'timestamptz', 'datetime2', 'datetimeoffset'].includes(type)) {
    return TypeCategory.DATETIME;
  }

  // Time types
  if (['time', 'timetz'].includes(type)) {
    return TypeCategory.TIME;
  }

  // Binary types
  if (['binary', 'varbinary', 'blob', 'bytea', 'image'].includes(type)) {
    return TypeCategory.BINARY;
  }

  // JSON types
  if (['json', 'jsonb'].includes(type)) {
    return TypeCategory.JSON;
  }

  // UUID types
  if (['uuid', 'uniqueidentifier'].includes(type)) {
    return TypeCategory.UUID;
  }

  return TypeCategory.UNKNOWN;
}

/**
 * Extract varchar/char length from type string
 */
function extractTypeLength(column: SchemaColumn): number | null {
  if (column.varcharLength || column.varchar_length) {
    return column.varcharLength || column.varchar_length || null;
  }

  // Try to extract from dataType string
  const dataType = column.dataType || column.type || '';
  const match = dataType.match(/\((\d+)\)/);
  return match ? parseInt(match[1], 10) : null;
}

/**
 * Check if types are compatible for direct mapping
 */
function areTypesCompatible(sourceCategory: TypeCategory, targetCategory: TypeCategory): boolean {
  // Same category is always compatible
  if (sourceCategory === targetCategory) {
    return true;
  }

  // Compatible numeric conversions
  if (sourceCategory === TypeCategory.INTEGER && targetCategory === TypeCategory.FLOAT) {
    return true;
  }

  // Date/time conversions
  if (sourceCategory === TypeCategory.DATE && targetCategory === TypeCategory.DATETIME) {
    return true;
  }

  // Everything can be converted to string
  if (targetCategory === TypeCategory.STRING) {
    return true;
  }

  return false;
}

/**
 * Check if conversion needs a transformation
 */
function needsTransformation(sourceCategory: TypeCategory, targetCategory: TypeCategory): boolean {
  // Same category doesn't need transformation
  if (sourceCategory === targetCategory) {
    return false;
  }

  // Compatible conversions that databases handle automatically
  if (sourceCategory === TypeCategory.INTEGER && targetCategory === TypeCategory.FLOAT) {
    return false;
  }
  
  if (sourceCategory === TypeCategory.DATE && targetCategory === TypeCategory.DATETIME) {
    return false;
  }

  // Most other conversions need explicit transformation
  return true;
}

/**
 * Check for potential data loss in the conversion
 */
function checkDataLoss(sourceColumn: SchemaColumn, targetColumn: SchemaColumn): { hasDataLoss: boolean; reasons: string[] } {
  const reasons: string[] = [];
  let hasDataLoss = false;

  const sourceType = sourceColumn.dataType || sourceColumn.type || '';
  const targetType = targetColumn.dataType || targetColumn.type || '';
  const sourceCategory = getTypeCategory(sourceType);
  const targetCategory = getTypeCategory(targetType);

  // Check string length reduction
  if (sourceCategory === TypeCategory.STRING && targetCategory === TypeCategory.STRING) {
    const sourceLength = extractTypeLength(sourceColumn);
    const targetLength = extractTypeLength(targetColumn);
    
    if (sourceLength && targetLength && sourceLength > targetLength) {
      hasDataLoss = true;
      reasons.push(`String length reduction: ${sourceLength} → ${targetLength} may truncate data`);
    }
  }

  // Check numeric precision loss
  if (sourceCategory === TypeCategory.FLOAT && targetCategory === TypeCategory.INTEGER) {
    hasDataLoss = true;
    reasons.push('Float to integer conversion will lose decimal precision');
  }

  // Check datetime to date conversion
  if (sourceCategory === TypeCategory.DATETIME && targetCategory === TypeCategory.DATE) {
    hasDataLoss = true;
    reasons.push('DateTime to Date conversion will lose time information');
  }

  // Check nullable constraint mismatch
  const sourceNullable = sourceColumn.isNullable !== false;
  const targetNullable = targetColumn.isNullable !== false;
  
  if (sourceNullable && !targetNullable) {
    hasDataLoss = true;
    reasons.push('Source allows NULL but target does not - NULL values will be rejected');
  }

  return { hasDataLoss, reasons };
}

/**
 * Validates a mapping rule by comparing source and target column types
 * @param sourceColumn - Source column schema information
 * @param targetColumn - Target column schema information
 * @param transformationName - Name of transformation being applied (if any)
 * @returns Validation result with details
 */
export function validateMappingRule(
  sourceColumn: SchemaColumn | null,
  targetColumn: SchemaColumn | null,
  transformationName?: string | null
): ValidationResult {
  // If we don't have column info, we can't validate
  if (!sourceColumn || !targetColumn) {
    return {
      isValid: true,
      level: 'info',
      message: 'Schema information not available for validation',
    };
  }

  const sourceType = sourceColumn.dataType || sourceColumn.type || 'unknown';
  const targetType = targetColumn.dataType || targetColumn.type || 'unknown';
  const sourceCategory = getTypeCategory(sourceType);
  const targetCategory = getTypeCategory(targetType);

  const issues: string[] = [];

  // Check type compatibility
  const compatible = areTypesCompatible(sourceCategory, targetCategory);
  const needsTransform = needsTransformation(sourceCategory, targetCategory);
  const { hasDataLoss, reasons: dataLossReasons } = checkDataLoss(sourceColumn, targetColumn);

  // Build the validation result
  let level: ValidationLevel = 'success';
  let message = 'Mapping is valid';
  let isValid = true;

  // Check for incompatible types
  if (!compatible) {
    level = 'error';
    message = `Incompatible types: ${sourceType} cannot be converted to ${targetType}`;
    issues.push(message);
    isValid = false;
  } 
  // Check if transformation is needed but not provided
  else if (needsTransform && !transformationName) {
    level = 'warning';
    message = `Type conversion needed: ${sourceType} → ${targetType} requires a transformation`;
    issues.push(message);
  }
  // Check for potential data loss
  else if (hasDataLoss) {
    level = 'warning';
    message = 'Potential data loss in this mapping';
    issues.push(...dataLossReasons);
  }
  // Transformation on compatible types is fine - no warning needed
  else if (transformationName && !needsTransform && transformationName !== 'direct_mapping') {
    level = 'success';
    message = `Valid mapping with transformation: ${sourceType} → ${targetType}`;
  }
  // Everything looks good
  else if (compatible) {
    level = 'success';
    message = needsTransform && transformationName
      ? `Valid mapping with transformation: ${sourceType} → ${targetType}`
      : `Valid direct mapping: ${sourceType} → ${targetType}`;
  }

  return {
    isValid,
    level,
    message,
    details: {
      sourceType,
      targetType,
      transformationNeeded: needsTransform,
      potentialDataLoss: hasDataLoss,
      issues,
    },
  };
}

/**
 * Validates multiple mapping rules
 * @param rules - Array of rule validation inputs
 * @returns Array of validation results
 */
export function validateMappingRules(
  rules: Array<{
    sourceColumn: SchemaColumn | null;
    targetColumn: SchemaColumn | null;
    transformationName?: string | null;
  }>
): ValidationResult[] {
  return rules.map((rule) =>
    validateMappingRule(rule.sourceColumn, rule.targetColumn, rule.transformationName)
  );
}

/**
 * Get a summary of validation results
 */
export function getValidationSummary(results: ValidationResult[]): {
  total: number;
  valid: number;
  warnings: number;
  errors: number;
  info: number;
} {
  return {
    total: results.length,
    valid: results.filter((r) => r.level === 'success').length,
    warnings: results.filter((r) => r.level === 'warning').length,
    errors: results.filter((r) => r.level === 'error').length,
    info: results.filter((r) => r.level === 'info').length,
  };
}

