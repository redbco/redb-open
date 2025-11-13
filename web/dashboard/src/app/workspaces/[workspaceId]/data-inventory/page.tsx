'use client';

import { useState } from 'react';
import { Package, Filter, Plus, Table, Database, Box, Pencil } from 'lucide-react';
import { useDataInventory } from '@/lib/hooks/useResourceInventory';
import { LoadingSpinner } from '@/components/ui/LoadingSpinner';
import { ErrorState } from '@/components/ui/ErrorState';
import { EmptyState } from '@/components/ui/EmptyState';
import type { ResourceItem, ResourceContainer } from '@/lib/api/types';
import { api } from '@/lib/api/endpoints';

interface DataInventoryPageProps {
  params: Promise<{
    workspaceId: string;
  }>;
}

export default function DataInventoryPage({ params }: DataInventoryPageProps) {
  const [workspaceId, setWorkspaceId] = useState<string>('');
  const [activeView, setActiveView] = useState<'items' | 'products'>('items');
  const [showCreateDialog, setShowCreateDialog] = useState(false);
  const [editingItem, setEditingItem] = useState<ResourceItem | null>(null);
  const [editDisplayName, setEditDisplayName] = useState('');
  const [isSaving, setIsSaving] = useState(false);
  
  // Initialize params
  useState(() => {
    params.then(({ workspaceId: id }) => setWorkspaceId(id));
  });

  const inventory = useDataInventory(workspaceId);

  const handleEditClick = (item: ResourceItem, e: React.MouseEvent) => {
    e.stopPropagation();
    setEditingItem(item);
    setEditDisplayName(item.item_display_name || item.item_name);
  };

  const handleSaveDisplayName = async () => {
    if (!editingItem) return;

    setIsSaving(true);
    try {
      await api.resources.modifyItem(workspaceId, editingItem.item_id, {
        item_display_name: editDisplayName,
      });
      // Refresh the items list
      await inventory.refetchItems();
      setEditingItem(null);
      setEditDisplayName('');
    } catch (err) {
      console.error('Failed to update display name:', err);
      alert('Failed to update display name');
    } finally {
      setIsSaving(false);
    }
  };

  if (!workspaceId) {
    return (
      <div className="flex items-center justify-center min-h-[400px]">
        <LoadingSpinner size="lg" />
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-background">
      {/* Header */}
      <div className="border-b border-border bg-card">
        <div className="container mx-auto px-6 py-4">
          <div className="flex items-center justify-between">
            <div className="flex items-center space-x-3">
              <div className="w-10 h-10 rounded-lg bg-primary/10 flex items-center justify-center">
                <Package className="w-5 h-5 text-primary" />
              </div>
              <div>
                <h1 className="text-2xl font-bold text-foreground">Data Inventory</h1>
                <p className="text-sm text-muted-foreground">Explore resource items and create data products</p>
              </div>
            </div>
            <button
              onClick={() => setShowCreateDialog(true)}
              disabled={inventory.selectedItems.length === 0}
              className="flex items-center space-x-2 px-4 py-2 bg-primary text-primary-foreground rounded-md hover:bg-primary/90 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
            >
              <Plus className="w-4 h-4" />
              <span>Create Data Product</span>
              {inventory.selectedItems.length > 0 && (
                <span className="ml-2 px-2 py-0.5 bg-primary-foreground/20 rounded-full text-xs">
                  {inventory.selectedItems.length}
                </span>
              )}
            </button>
          </div>
        </div>
      </div>

      {/* View Tabs */}
      <div className="border-b border-border bg-card">
        <div className="container mx-auto px-6">
          <div className="flex space-x-8">
            <button
              onClick={() => setActiveView('items')}
              className={`px-4 py-3 border-b-2 transition-colors ${
                activeView === 'items'
                  ? 'border-primary text-primary font-medium'
                  : 'border-transparent text-muted-foreground hover:text-foreground'
              }`}
            >
              <div className="flex items-center space-x-2">
                <Table className="w-4 h-4" />
                <span>Resource Items</span>
                <span className="px-2 py-0.5 bg-muted rounded-full text-xs">
                  {inventory.items.length}
                </span>
              </div>
            </button>
            <button
              onClick={() => setActiveView('products')}
              className={`px-4 py-3 border-b-2 transition-colors ${
                activeView === 'products'
                  ? 'border-primary text-primary font-medium'
                  : 'border-transparent text-muted-foreground hover:text-foreground'
              }`}
            >
              <div className="flex items-center space-x-2">
                <Box className="w-4 h-4" />
                <span>Data Products</span>
                <span className="px-2 py-0.5 bg-muted rounded-full text-xs">
                  {inventory.dataProducts.length}
                </span>
              </div>
            </button>
          </div>
        </div>
      </div>

      {/* Main Content */}
      <div className="container mx-auto px-6 py-6">
        <div className="flex gap-6">
          {/* Filters Sidebar */}
          <aside className="w-64 flex-shrink-0">
            <div className="sticky top-6 bg-card border border-border rounded-lg p-4">
              <div className="flex items-center space-x-2 mb-4">
                <Filter className="w-4 h-4 text-muted-foreground" />
                <h3 className="font-medium text-foreground">Filters</h3>
              </div>
              
              {activeView === 'items' && (
                <div className="space-y-4">
                  {/* Protocol Filter */}
                  <div>
                    <label className="text-xs font-medium text-muted-foreground mb-2 block">Protocol</label>
                    <select
                      className="w-full px-3 py-2 bg-background border border-border rounded-md text-sm"
                      onChange={(e) => inventory.setItemFilters(prev => ({ ...prev, protocol: e.target.value || undefined }))}
                    >
                      <option value="">All</option>
                      <option value="redb">redb</option>
                      <option value="stream">stream</option>
                      <option value="webhook">webhook</option>
                      <option value="mcp">mcp</option>
                    </select>
                  </div>

                  {/* Item Type Filter */}
                  <div>
                    <label className="text-xs font-medium text-muted-foreground mb-2 block">Item Type</label>
                    <select
                      className="w-full px-3 py-2 bg-background border border-border rounded-md text-sm"
                      onChange={(e) => inventory.setItemFilters(prev => ({ ...prev, item_type: e.target.value || undefined }))}
                    >
                      <option value="">All</option>
                      <option value="column">column</option>
                      <option value="field">field</option>
                      <option value="property">property</option>
                    </select>
                  </div>

                  {/* Attribute Filters */}
                  <div className="pt-2 border-t border-border">
                    <label className="text-xs font-medium text-muted-foreground mb-2 block">Attributes</label>
                    <div className="space-y-2">
                      <label className="flex items-center space-x-2 cursor-pointer">
                        <input
                          type="checkbox"
                          className="rounded border-border"
                          onChange={(e) => inventory.setItemFilters(prev => ({ ...prev, is_primary_key: e.target.checked || undefined }))}
                        />
                        <span className="text-sm">Primary Key</span>
                      </label>
                      <label className="flex items-center space-x-2 cursor-pointer">
                        <input
                          type="checkbox"
                          className="rounded border-border"
                          onChange={(e) => inventory.setItemFilters(prev => ({ ...prev, is_unique: e.target.checked || undefined }))}
                        />
                        <span className="text-sm">Unique</span>
                      </label>
                      <label className="flex items-center space-x-2 cursor-pointer">
                        <input
                          type="checkbox"
                          className="rounded border-border"
                          onChange={(e) => inventory.setItemFilters(prev => ({ ...prev, is_indexed: e.target.checked || undefined }))}
                        />
                        <span className="text-sm">Indexed</span>
                      </label>
                      <label className="flex items-center space-x-2 cursor-pointer">
                        <input
                          type="checkbox"
                          className="rounded border-border"
                          onChange={(e) => inventory.setItemFilters(prev => ({ ...prev, is_privileged: e.target.checked || undefined }))}
                        />
                        <span className="text-sm">Privileged</span>
                      </label>
                    </div>
                  </div>

                  {/* Selection Actions */}
                  {inventory.selectedItems.length > 0 && (
                    <div className="pt-2 border-t border-border">
                      <button
                        onClick={inventory.clearSelection}
                        className="w-full px-3 py-2 text-sm bg-muted text-foreground rounded-md hover:bg-muted/80 transition-colors"
                      >
                        Clear Selection
                      </button>
                    </div>
                  )}
                </div>
              )}
            </div>
          </aside>

          {/* Main Content Area */}
          <main className="flex-1">
            {activeView === 'items' && (
              <div className="space-y-4">
                {inventory.itemsLoading ? (
                  <div className="flex items-center justify-center py-12">
                    <LoadingSpinner size="lg" />
                  </div>
                ) : inventory.itemsError ? (
                  <ErrorState message={inventory.itemsError} />
                ) : inventory.items.length === 0 ? (
                  <EmptyState
                    icon={Database}
                    title="No resource items found"
                    message="Connect databases to see resource items here"
                  />
                ) : (
                  <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
                    {inventory.items.map((item) => (
                      <div
                        key={item.item_id}
                        className={`group bg-card border rounded-lg p-4 transition-all cursor-pointer hover:border-primary/50 ${
                          inventory.selectedItems.some(i => i.item_id === item.item_id)
                            ? 'border-primary bg-primary/5'
                            : 'border-border'
                        }`}
                        onClick={() => inventory.toggleItemSelection(item)}
                      >
                        <div className="flex items-start justify-between mb-2">
                          <div className="flex-1">
                            <div className="flex items-center gap-2 mb-1">
                              <h4 className="font-medium text-foreground truncate">{item.item_display_name || item.item_name}</h4>
                              <button
                                onClick={(e) => handleEditClick(item, e)}
                                className="opacity-0 group-hover:opacity-100 hover:bg-muted p-1 rounded transition-opacity"
                                title="Edit display name"
                              >
                                <Pencil className="w-3 h-3 text-muted-foreground" />
                              </button>
                            </div>
                            <p className="text-xs text-muted-foreground">{item.data_type}</p>
                          </div>
                          <input
                            type="checkbox"
                            checked={inventory.selectedItems.some(i => i.item_id === item.item_id)}
                            onChange={() => inventory.toggleItemSelection(item)}
                            onClick={(e) => e.stopPropagation()}
                            className="rounded border-border"
                          />
                        </div>
                        <div className="flex flex-wrap gap-1 mt-2">
                          {item.is_primary_key && (
                            <span className="px-2 py-0.5 bg-blue-500/10 text-blue-500 text-xs rounded-full">PK</span>
                          )}
                          {item.is_unique && (
                            <span className="px-2 py-0.5 bg-purple-500/10 text-purple-500 text-xs rounded-full">Unique</span>
                          )}
                          {item.is_indexed && (
                            <span className="px-2 py-0.5 bg-green-500/10 text-green-500 text-xs rounded-full">Indexed</span>
                          )}
                          {item.is_privileged && (
                            <span className="px-2 py-0.5 bg-red-500/10 text-red-500 text-xs rounded-full">Privileged</span>
                          )}
                        </div>
                      </div>
                    ))}
                  </div>
                )}
              </div>
            )}

            {activeView === 'products' && (
              <div className="space-y-4">
                {inventory.dataProductsLoading ? (
                  <div className="flex items-center justify-center py-12">
                    <LoadingSpinner size="lg" />
                  </div>
                ) : inventory.dataProductsError ? (
                  <ErrorState message={inventory.dataProductsError} />
                ) : inventory.dataProducts.length === 0 ? (
                  <EmptyState
                    icon={Box}
                    title="No data products yet"
                    message="Select resource items and create your first data product"
                  />
                ) : (
                  <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
                    {inventory.dataProducts.map((product) => (
                      <div
                        key={product.product_id}
                        className="bg-card border border-border rounded-lg p-4 hover:border-primary/50 transition-all"
                      >
                        <div className="flex items-start justify-between mb-2">
                          <div>
                            <h4 className="font-medium text-foreground">{product.product_name}</h4>
                            <p className="text-sm text-muted-foreground mt-1">{product.product_description}</p>
                          </div>
                        </div>
                        <div className="mt-3 pt-3 border-t border-border">
                          <p className="text-xs text-muted-foreground">
                            {product.resource_items?.length || 0} resource items
                          </p>
                        </div>
                      </div>
                    ))}
                  </div>
                )}
              </div>
            )}
          </main>
        </div>
      </div>

      {/* Simple Create Dialog */}
      {showCreateDialog && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
          <div className="bg-card border border-border rounded-lg p-6 w-full max-w-md">
            <h3 className="text-lg font-semibold text-foreground mb-4">Create Data Product</h3>
            <form
              onSubmit={async (e) => {
                e.preventDefault();
                const formData = new FormData(e.currentTarget);
                const productName = formData.get('product_name') as string;
                const productDescription = formData.get('product_description') as string;
                
                try {
                  await inventory.createDataProduct({
                    product_name: productName,
                    product_description: productDescription,
                    resource_item_ids: inventory.selectedItems.map(i => i.item_id),
                  });
                  setShowCreateDialog(false);
                  inventory.clearSelection();
                  setActiveView('products');
                } catch (err) {
                  console.error('Failed to create data product:', err);
                }
              }}
            >
              <div className="space-y-4">
                <div>
                  <label className="block text-sm font-medium text-foreground mb-2">
                    Product Name
                  </label>
                  <input
                    type="text"
                    name="product_name"
                    required
                    className="w-full px-3 py-2 bg-background border border-border rounded-md text-sm"
                    placeholder="my-data-product"
                  />
                </div>
                <div>
                  <label className="block text-sm font-medium text-foreground mb-2">
                    Description
                  </label>
                  <textarea
                    name="product_description"
                    rows={3}
                    className="w-full px-3 py-2 bg-background border border-border rounded-md text-sm"
                    placeholder="Describe your data product..."
                  />
                </div>
                <div className="text-sm text-muted-foreground">
                  {inventory.selectedItems.length} resource items selected
                </div>
              </div>
              <div className="flex justify-end space-x-3 mt-6">
                <button
                  type="button"
                  onClick={() => setShowCreateDialog(false)}
                  className="px-4 py-2 text-sm border border-border rounded-md hover:bg-muted transition-colors"
                >
                  Cancel
                </button>
                <button
                  type="submit"
                  className="px-4 py-2 text-sm bg-primary text-primary-foreground rounded-md hover:bg-primary/90 transition-colors"
                >
                  Create Product
                </button>
              </div>
            </form>
          </div>
        </div>
      )}

      {/* Edit Display Name Dialog */}
      {editingItem && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
          <div className="bg-card border border-border rounded-lg p-6 w-full max-w-md">
            <h3 className="text-lg font-semibold text-foreground mb-4">Edit Display Name</h3>
            <div className="space-y-4">
              <div>
                <label className="block text-sm font-medium text-muted-foreground mb-2">
                  Original Name
                </label>
                <p className="text-sm text-foreground px-3 py-2 bg-muted rounded-md">
                  {editingItem.item_name}
                </p>
              </div>
              <div>
                <label className="block text-sm font-medium text-foreground mb-2">
                  Display Name
                </label>
                <input
                  type="text"
                  value={editDisplayName}
                  onChange={(e) => setEditDisplayName(e.target.value)}
                  className="w-full px-3 py-2 bg-background border border-border rounded-md text-sm"
                  placeholder="Enter a friendly display name"
                  autoFocus
                />
              </div>
            </div>
            <div className="flex justify-end space-x-3 mt-6">
              <button
                type="button"
                onClick={() => {
                  setEditingItem(null);
                  setEditDisplayName('');
                }}
                disabled={isSaving}
                className="px-4 py-2 text-sm border border-border rounded-md hover:bg-muted transition-colors disabled:opacity-50"
              >
                Cancel
              </button>
              <button
                onClick={handleSaveDisplayName}
                disabled={isSaving || !editDisplayName.trim()}
                className="px-4 py-2 text-sm bg-primary text-primary-foreground rounded-md hover:bg-primary/90 transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
              >
                {isSaving ? 'Saving...' : 'Save'}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

