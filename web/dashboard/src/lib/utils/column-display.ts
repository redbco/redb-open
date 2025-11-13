import type { ResourceItem } from '@/lib/api/types';

/**
 * Get the display name for a column, either the user-defined display name or the real column name
 * @param columnName The real column name
 * @param items Array of resource items to search through
 * @param useDisplayName Whether to use the display name (true) or real name (false)
 * @returns The display name if available and useDisplayName is true, otherwise the real column name
 */
export function getColumnDisplayName(
  columnName: string,
  items: ResourceItem[] | undefined,
  useDisplayName: boolean
): string {
  if (!useDisplayName || !items || items.length === 0) {
    return columnName;
  }

  const item = items.find((i) => i.item_name === columnName);
  return item?.item_display_name || columnName;
}

/**
 * Get the display label for a full column path (table.column)
 * @param tableName The table name
 * @param columnName The column name
 * @param items Array of resource items to search through
 * @param useDisplayName Whether to use the display name (true) or real name (false)
 * @returns The formatted display label with table and column names
 */
export function getFullColumnDisplayLabel(
  tableName: string | null,
  columnName: string,
  items: ResourceItem[] | undefined,
  useDisplayName: boolean
): string {
  const displayName = getColumnDisplayName(columnName, items, useDisplayName);
  
  if (tableName) {
    return `${tableName}.${displayName}`;
  }
  
  return displayName;
}

