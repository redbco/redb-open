import { useState, useEffect } from 'react';
import { api } from '../api/endpoints';
import type {
  ResourceContainer,
  ResourceItem,
  ResourceContainerFilter,
  ResourceItemFilter,
  DataProduct,
  CreateDataProductRequest,
  ModifyDataProductRequest,
} from '../api/types';

// Hook for managing resource containers
export function useResourceContainers(workspaceName: string, filters?: ResourceContainerFilter) {
  const [containers, setContainers] = useState<ResourceContainer[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const fetchContainers = async () => {
    if (!workspaceName) return;
    
    setIsLoading(true);
    setError(null);
    try {
      const response = await api.resources.listContainers(workspaceName, filters);
      setContainers(response.containers || []);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to fetch containers');
    } finally {
      setIsLoading(false);
    }
  };

  useEffect(() => {
    fetchContainers();
  }, [workspaceName, JSON.stringify(filters)]);

  return {
    containers,
    isLoading,
    error,
    refetch: fetchContainers,
  };
}

// Hook for managing resource items
export function useResourceItems(workspaceName: string, filters?: ResourceItemFilter) {
  const [items, setItems] = useState<ResourceItem[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const fetchItems = async () => {
    if (!workspaceName) return;
    
    setIsLoading(true);
    setError(null);
    try {
      const response = await api.resources.listItems(workspaceName, filters);
      setItems(response.items || []);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to fetch items');
    } finally {
      setIsLoading(false);
    }
  };

  useEffect(() => {
    fetchItems();
  }, [workspaceName, JSON.stringify(filters)]);

  return {
    items,
    isLoading,
    error,
    refetch: fetchItems,
  };
}

// Hook for managing data products
export function useDataProducts(workspaceName: string) {
  const [dataProducts, setDataProducts] = useState<DataProduct[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const fetchDataProducts = async () => {
    if (!workspaceName) return;
    
    setIsLoading(true);
    setError(null);
    try {
      const response = await api.dataProducts.list(workspaceName);
      setDataProducts(response.dataproducts || []);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to fetch data products');
    } finally {
      setIsLoading(false);
    }
  };

  const createDataProduct = async (request: CreateDataProductRequest) => {
    setError(null);
    try {
      const response = await api.dataProducts.create(workspaceName, request);
      await fetchDataProducts(); // Refresh the list
      return response.dataproduct;
    } catch (err) {
      const errorMsg = err instanceof Error ? err.message : 'Failed to create data product';
      setError(errorMsg);
      throw new Error(errorMsg);
    }
  };

  const updateDataProduct = async (productName: string, request: ModifyDataProductRequest) => {
    setError(null);
    try {
      const response = await api.dataProducts.modify(workspaceName, productName, request);
      await fetchDataProducts(); // Refresh the list
      return response.dataproduct;
    } catch (err) {
      const errorMsg = err instanceof Error ? err.message : 'Failed to update data product';
      setError(errorMsg);
      throw new Error(errorMsg);
    }
  };

  const deleteDataProduct = async (productName: string) => {
    setError(null);
    try {
      await api.dataProducts.delete(workspaceName, productName);
      await fetchDataProducts(); // Refresh the list
    } catch (err) {
      const errorMsg = err instanceof Error ? err.message : 'Failed to delete data product';
      setError(errorMsg);
      throw new Error(errorMsg);
    }
  };

  useEffect(() => {
    fetchDataProducts();
  }, [workspaceName]);

  return {
    dataProducts,
    isLoading,
    error,
    refetch: fetchDataProducts,
    createDataProduct,
    updateDataProduct,
    deleteDataProduct,
  };
}

// Combined hook for the data inventory page
export function useDataInventory(workspaceName: string) {
  const [containerFilters, setContainerFilters] = useState<ResourceContainerFilter>({});
  const [itemFilters, setItemFilters] = useState<ResourceItemFilter>({});
  const [selectedItems, setSelectedItems] = useState<ResourceItem[]>([]);

  const containers = useResourceContainers(workspaceName, containerFilters);
  const items = useResourceItems(workspaceName, itemFilters);
  const dataProducts = useDataProducts(workspaceName);

  const toggleItemSelection = (item: ResourceItem) => {
    setSelectedItems(prev => {
      const isSelected = prev.some(i => i.item_id === item.item_id);
      if (isSelected) {
        return prev.filter(i => i.item_id !== item.item_id);
      } else {
        return [...prev, item];
      }
    });
  };

  const clearSelection = () => {
    setSelectedItems([]);
  };

  const selectAll = () => {
    setSelectedItems(items.items);
  };

  return {
    // Container data
    containers: containers.containers,
    containersLoading: containers.isLoading,
    containersError: containers.error,
    refetchContainers: containers.refetch,
    containerFilters,
    setContainerFilters,

    // Item data
    items: items.items,
    itemsLoading: items.isLoading,
    itemsError: items.error,
    refetchItems: items.refetch,
    itemFilters,
    setItemFilters,

    // Selection
    selectedItems,
    toggleItemSelection,
    clearSelection,
    selectAll,

    // Data products
    dataProducts: dataProducts.dataProducts,
    dataProductsLoading: dataProducts.isLoading,
    dataProductsError: dataProducts.error,
    refetchDataProducts: dataProducts.refetch,
    createDataProduct: dataProducts.createDataProduct,
    updateDataProduct: dataProducts.updateDataProduct,
    deleteDataProduct: dataProducts.deleteDataProduct,
  };
}

