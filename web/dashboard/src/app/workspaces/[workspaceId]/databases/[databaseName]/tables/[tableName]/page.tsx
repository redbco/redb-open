'use client';

import { useState, useEffect } from 'react';
import { useRouter } from 'next/navigation';
import Link from 'next/link';
import { api } from '@/lib/api/endpoints';
import { LoadingSpinner } from '@/components/ui/LoadingSpinner';
import { Table, ArrowLeft, Lock, Unlock, Key, Shield, ChevronDown, ChevronUp, Database } from 'lucide-react';
import { Button } from '@/components/ui/Button';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/Select';
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from '@/components/ui/Tooltip';
import type { TableDataRow, TableColumnSchema } from '@/lib/api/types';

interface TableDataPageProps {
  params: Promise<{
    workspaceId: string;
    databaseName: string;
    tableName: string;
  }>;
}

export default function TableDataPage({ params }: TableDataPageProps) {
  const router = useRouter();

  const [workspaceId, setWorkspaceId] = useState<string>('');
  const [databaseName, setDatabaseName] = useState<string>('');
  const [tableName, setTableName] = useState<string>('');
  const [data, setData] = useState<TableDataRow[]>([]);
  const [columnSchemas, setColumnSchemas] = useState<TableColumnSchema[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [currentPage, setCurrentPage] = useState(1);
  const [pageSize, setPageSize] = useState(25);
  const [totalRows, setTotalRows] = useState(0);
  const [totalPages, setTotalPages] = useState(0);
  const [showPrivilegedData, setShowPrivilegedData] = useState(false);
  const [showColumnDetails, setShowColumnDetails] = useState(true);

  // Initialize params
  useEffect(() => {
    params.then(({ workspaceId: wid, databaseName: dbName, tableName: tblName }) => {
      setWorkspaceId(wid);
      setDatabaseName(dbName);
      setTableName(tblName);
    });
  }, [params]);

  useEffect(() => {
    if (!workspaceId || !databaseName || !tableName) {
      if (workspaceId !== '' || databaseName !== '' || tableName !== '') {
        setError('Missing parameters for fetching table data.');
        setIsLoading(false);
      }
      return;
    }

    const fetchTableData = async () => {
      setIsLoading(true);
      setError(null);
      try {
        const response = await api.databases.fetchTableData(
          workspaceId,
          databaseName,
          tableName,
          currentPage,
          pageSize
        );
        if (response.success) {
          setData(response.data || []);
          setColumnSchemas(response.column_schemas || []);
          setTotalRows(response.total_rows || 0);
          setTotalPages(response.total_pages || 0);
        } else {
          setError(response.message || 'Failed to fetch table data.');
        }
      } catch (err) {
        console.error('Error fetching table data:', err);
        setError('Failed to fetch table data. Please try again.');
      } finally {
        setIsLoading(false);
      }
    };

    fetchTableData();
  }, [workspaceId, databaseName, tableName, currentPage, pageSize]);

  const getColumnNames = () => {
    if (columnSchemas.length > 0) {
      // Sort columns by ordinal_position before returning names
      return columnSchemas
        .sort((a, b) => (a.ordinal_position || 0) - (b.ordinal_position || 0))
        .map(col => col.name);
    }
    if (data.length === 0) return [];
    return Object.keys(data[0]);
  };

  const getColumnSchema = (columnName: string): TableColumnSchema | undefined => {
    return columnSchemas.find(col => col.name === columnName);
  };

  const isColumnPrivileged = (columnName: string) => {
    const schema = getColumnSchema(columnName);
    return schema?.is_privileged || false;
  };

  // Check if column should be obfuscated (high confidence privileged data)
  const isHighConfidencePrivileged = (columnName: string) => {
    const schema = getColumnSchema(columnName);
    if (!schema?.is_privileged) return false;
    
    const confidence = schema.privileged_confidence || 0;
    return confidence > 0.7; // High confidence threshold
  };

  const getPrivilegedConfidenceLevel = (columnName: string): 'high' | 'medium' | 'low' | null => {
    const schema = getColumnSchema(columnName);
    if (!schema?.is_privileged) return null;
    
    const confidence = schema.privileged_confidence || 0;
    if (confidence > 0.7) return 'high';
    if (confidence >= 0.4) return 'medium';
    if (confidence > 0) return 'low';
    return null;
  };

  const handlePageChange = (newPage: number) => {
    if (newPage > 0 && newPage <= totalPages) {
      setCurrentPage(newPage);
    }
  };

  const handlePageSizeChange = (newSize: string) => {
    const size = parseInt(newSize);
    if (!isNaN(size) && size > 0) {
      setPageSize(size);
      setCurrentPage(1); // Reset to first page when page size changes
    }
  };

  const renderColumnHeader = (colName: string) => {
    const schema = getColumnSchema(colName);
    const isPrivileged = schema?.is_privileged || false;
    const classification = schema?.privileged_classification || 'Sensitive';
    const dataCategory = schema?.data_category;
    const confidenceLevel = getPrivilegedConfidenceLevel(colName);
    const confidence = schema?.privileged_confidence || 0;

    let privilegedColor = 'text-red-500 dark:text-red-400';
    
    if (isPrivileged && confidenceLevel) {
      if (confidenceLevel === 'high') {
        privilegedColor = 'text-red-500 dark:text-red-400';
      } else if (confidenceLevel === 'medium') {
        privilegedColor = 'text-orange-500 dark:text-orange-400';
      } else if (confidenceLevel === 'low') {
        privilegedColor = 'text-yellow-500 dark:text-yellow-400';
      }
    }

    return (
      <th scope="col" className="px-4 py-4 text-left" key={colName}>
        <div className="flex flex-col gap-2">
          {/* Column Name - Primary Focus */}
          <div className="flex items-center gap-2">
            <span className="font-bold text-sm text-foreground">{colName}</span>
            
            {/* Key Indicators - Icon Only */}
            <div className="flex items-center gap-1">
              {schema?.is_primary_key && (
                <Tooltip>
                  <TooltipTrigger asChild>
                    <div className="flex items-center justify-center w-5 h-5 rounded bg-blue-100 dark:bg-blue-900/30">
                      <Key className="h-3 w-3 text-blue-600 dark:text-blue-400" />
                    </div>
                  </TooltipTrigger>
                  <TooltipContent>
                    <p className="font-semibold">Primary Key</p>
                    <p className="text-xs text-muted-foreground">Unique identifier for this table</p>
                  </TooltipContent>
                </Tooltip>
              )}
              
              {isPrivileged && (
                <Tooltip>
                  <TooltipTrigger asChild>
                    <div className={`flex items-center justify-center w-5 h-5 rounded ${
                      confidenceLevel === 'high' ? 'bg-red-100 dark:bg-red-900/30' :
                      confidenceLevel === 'medium' ? 'bg-orange-100 dark:bg-orange-900/30' :
                      'bg-yellow-100 dark:bg-yellow-900/30'
                    }`}>
                      <Shield className={`h-3 w-3 ${privilegedColor}`} />
                    </div>
                  </TooltipTrigger>
                  <TooltipContent>
                    <div className="max-w-xs">
                      <p className="font-semibold">{classification} Data</p>
                      <p className="text-xs mt-1">
                        Confidence: {(confidence * 100).toFixed(0)}% ({confidenceLevel})
                      </p>
                      {confidenceLevel === 'high' && (
                        <p className="text-xs mt-1 text-red-400">
                          🔒 High confidence
                        </p>
                      )}
                      {confidenceLevel === 'medium' && (
                        <p className="text-xs mt-1 text-orange-400">
                          ⚠️ Medium confidence
                        </p>
                      )}
                      {confidenceLevel === 'low' && (
                        <p className="text-xs mt-1 text-yellow-400">
                          ℹ️ Low confidence
                        </p>
                      )}
                    </div>
                  </TooltipContent>
                </Tooltip>
              )}
            </div>
          </div>
          
          {/* Type and Constraints - Secondary Info */}
          {showColumnDetails && schema && (
            <div className="flex flex-col gap-1.5">
              {/* Data Type Row */}
              <div className="flex items-center gap-1.5">
                <span className="text-[11px] font-mono text-muted-foreground bg-muted px-1.5 py-0.5 rounded">
                  {schema.data_type}
                </span>
                {!schema.is_nullable && (
                  <Tooltip>
                    <TooltipTrigger asChild>
                      <span className="text-[10px] px-1.5 py-0.5 rounded bg-gray-200 dark:bg-gray-800 text-gray-700 dark:text-gray-300 font-medium cursor-help">
                        NOT NULL
                      </span>
                    </TooltipTrigger>
                    <TooltipContent>
                      <p>This column cannot contain NULL values</p>
                    </TooltipContent>
                  </Tooltip>
                )}
              </div>
              
              {/* Constraints Row - Only show if any exist */}
              {(schema.is_unique || schema.is_indexed) && (
                <div className="flex items-center gap-1.5 flex-wrap">
                  {schema.is_unique && (
                    <Tooltip>
                      <TooltipTrigger asChild>
                        <span className="text-[10px] px-1.5 py-0.5 rounded bg-indigo-100 dark:bg-indigo-900/30 text-indigo-700 dark:text-indigo-300 font-medium cursor-help">
                          UNIQUE
                        </span>
                      </TooltipTrigger>
                      <TooltipContent>
                        <p>All values in this column must be unique</p>
                      </TooltipContent>
                    </Tooltip>
                  )}
                  {schema.is_indexed && (
                    <Tooltip>
                      <TooltipTrigger asChild>
                        <span className="text-[10px] px-1.5 py-0.5 rounded bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-300 font-medium cursor-help">
                          INDEXED
                        </span>
                      </TooltipTrigger>
                      <TooltipContent>
                        <p>This column has an index for faster queries</p>
                      </TooltipContent>
                    </Tooltip>
                  )}
                </div>
              )}
              
              {/* Data Category - Only show if not standard */}
              {dataCategory && dataCategory !== 'standard' && (
                <span className="text-[10px] text-blue-600 dark:text-blue-400 capitalize font-medium">
                  {dataCategory}
                </span>
              )}
            </div>
          )}
        </div>
      </th>
    );
  };

  if (isLoading) {
    return (
      <div className="flex justify-center items-center h-64">
        <LoadingSpinner />
      </div>
    );
  }

  if (error) {
    return (
      <div className="p-8 text-center text-red-500">
        <p className="text-lg font-semibold">Error: {error}</p>
        <Button onClick={() => router.back()} className="mt-4">
          Go Back
        </Button>
      </div>
    );
  }

  const columnNames = getColumnNames();
  const hasPrivilegedColumns = columnSchemas.some(col => col.is_privileged);

  return (
    <TooltipProvider>
      <div className="container mx-auto p-6">
      {/* Header */}
      <div className="flex items-center justify-between mb-6">
        <div className="flex items-center gap-4">
          <Link
            href={`/workspaces/${workspaceId}/databases/${databaseName}/schema`}
            className="inline-flex items-center text-muted-foreground hover:text-foreground transition-colors"
          >
            <ArrowLeft className="h-4 w-4 mr-2" />
            Back to Schema
          </Link>
        </div>
        
        <h1 className="text-3xl font-bold text-foreground flex items-center gap-2">
          <Table className="h-8 w-8" />
          {tableName}
        </h1>

        <div className="flex items-center gap-2">
          <Button
            variant="outline"
            size="sm"
            onClick={() => setShowColumnDetails(!showColumnDetails)}
            className="flex items-center gap-2"
          >
            {showColumnDetails ? (
              <>
                <ChevronUp className="h-4 w-4" /> Hide Details
              </>
            ) : (
              <>
                <ChevronDown className="h-4 w-4" /> Show Details
              </>
            )}
          </Button>
          {hasPrivilegedColumns && (
            <Button
              variant={showPrivilegedData ? "default" : "outline"}
              size="sm"
              onClick={() => setShowPrivilegedData(!showPrivilegedData)}
              className="flex items-center gap-2"
            >
              {showPrivilegedData ? (
                <>
                  <Unlock className="h-4 w-4" /> Hide Privileged
                </>
              ) : (
                <>
                  <Lock className="h-4 w-4" /> Show Privileged
                </>
              )}
            </Button>
          )}
        </div>
      </div>

      {/* Table Info Summary */}
      {columnSchemas.length > 0 && (
        <div className="mb-4 p-4 bg-muted/30 rounded-lg border border-border">
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4 text-sm">
            <div>
              <span className="text-muted-foreground">Total Columns:</span>
              <span className="ml-2 font-semibold">{columnSchemas.length}</span>
            </div>
            <div>
              <span className="text-muted-foreground">Privileged Columns:</span>
              <span className="ml-2 font-semibold text-red-600 dark:text-red-400">
                {columnSchemas.filter(col => col.is_privileged).length}
              </span>
            </div>
            <div>
              <span className="text-muted-foreground">Primary Keys:</span>
              <span className="ml-2 font-semibold text-blue-600 dark:text-blue-400">
                {columnSchemas.filter(col => col.is_primary_key).length}
              </span>
            </div>
            <div>
              <span className="text-muted-foreground">Total Rows:</span>
              <span className="ml-2 font-semibold">{totalRows}</span>
            </div>
          </div>
        </div>
      )}

      {data.length === 0 ? (
        <div className="bg-card border border-border rounded-lg p-12 text-center">
          <Database className="h-16 w-16 mx-auto text-muted-foreground mb-4" />
          <h3 className="text-xl font-semibold text-foreground mb-2">No Data Found</h3>
          <p className="text-muted-foreground">
            The table &quot;{tableName}&quot; in database &quot;{databaseName}&quot; contains no data.
          </p>
        </div>
      ) : (
        <>
          <div className="rounded-lg border border-border shadow-sm mb-6 bg-card">
            <div className="overflow-x-auto">
              <table className="w-full text-sm text-left text-muted-foreground">
                <thead className="text-xs text-foreground uppercase bg-muted/50 border-b border-border">
                  <tr>
                    {columnNames.map((colName) => renderColumnHeader(colName))}
                  </tr>
                </thead>
                <tbody>
                  {data.map((row, rowIndex) => (
                    <tr
                      key={rowIndex}
                      className="bg-card border-b border-border hover:bg-muted/30 transition-colors"
                    >
                      {columnNames.map((colName, colIndex) => {
                      const isPrivileged = isColumnPrivileged(colName);
                      const isHighConfidence = isHighConfidencePrivileged(colName);
                      const confidenceLevel = getPrivilegedConfidenceLevel(colName);
                      const cellValue = row[colName];
                      
                      // Only obfuscate high confidence privileged data
                      const shouldObfuscate = isHighConfidence && !showPrivilegedData;
                      const displayValue = shouldObfuscate
                        ? '••••••••' 
                        : (cellValue === null || cellValue === undefined) 
                          ? <span className="text-muted-foreground italic">NULL</span>
                          : String(cellValue);

                      // Apply styling based on confidence level
                      let cellClassName = 'px-6 py-4 font-medium whitespace-nowrap ';
                      if (shouldObfuscate) {
                        cellClassName += 'text-red-400 dark:text-red-500 font-mono';
                      } else if (isPrivileged && !showPrivilegedData) {
                        // Medium/low confidence - show data but with warning color
                        if (confidenceLevel === 'medium') {
                          cellClassName += 'text-orange-400 dark:text-orange-500';
                        } else if (confidenceLevel === 'low') {
                          cellClassName += 'text-yellow-400 dark:text-yellow-500';
                        }
                      } else {
                        cellClassName += 'text-foreground';
                      }

                      return (
                        <td 
                          className={cellClassName} 
                          key={colIndex}
                        >
                          {displayValue}
                        </td>
                      );
                    })}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          </div>

          {/* Pagination Controls */}
          <div className="flex flex-col sm:flex-row justify-between items-center gap-4 mt-6">
            <div className="flex items-center gap-2">
              <span className="text-sm text-muted-foreground">Rows per page:</span>
              <Select value={String(pageSize)} onValueChange={handlePageSizeChange}>
                <SelectTrigger className="w-[80px]">
                  <SelectValue placeholder={String(pageSize)} />
                </SelectTrigger>
                <SelectContent>
                  {[10, 25, 50, 100, 250].map((size) => (
                    <SelectItem key={size} value={String(size)}>
                      {size}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>

            <div className="flex flex-col sm:flex-row items-center gap-4">
              <span className="text-sm text-muted-foreground">
                Page {currentPage} of {totalPages} ({totalRows} rows total)
              </span>
              <div className="flex gap-2">
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => handlePageChange(currentPage - 1)}
                  disabled={currentPage === 1}
                >
                  Previous
                </Button>
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => handlePageChange(currentPage + 1)}
                  disabled={currentPage === totalPages}
                >
                  Next
                </Button>
              </div>
            </div>
          </div>
        </>
      )}
    </div>
    </TooltipProvider>
  );
}
