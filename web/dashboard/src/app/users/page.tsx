'use client';

import { useState } from 'react';
import { useUsers } from '@/lib/hooks/useUsers';
import { LoadingSpinner } from '@/components/ui/LoadingSpinner';
import { useToast } from '@/components/ui/Toast';
import { Users as UsersIcon, Plus, RefreshCw, Search } from 'lucide-react';
import { UserCard } from '@/components/users/UserCard';
import { AddUserDialog } from '@/components/users/AddUserDialog';

export default function UsersPage() {
  const [showAddDialog, setShowAddDialog] = useState(false);
  const [searchQuery, setSearchQuery] = useState('');
  const { showToast } = useToast();
  const { users, isLoading, error, refetch } = useUsers();

  const handleRefresh = () => {
    refetch();
    showToast({
      type: 'info',
      title: 'Refreshing users...',
    });
  };

  // Filter users based on search query
  const filteredUsers = users.filter(user => 
    user.user_name.toLowerCase().includes(searchQuery.toLowerCase()) ||
    user.user_email.toLowerCase().includes(searchQuery.toLowerCase()) ||
    (user.user_first_name && user.user_first_name.toLowerCase().includes(searchQuery.toLowerCase())) ||
    (user.user_last_name && user.user_last_name.toLowerCase().includes(searchQuery.toLowerCase()))
  );

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-3xl font-bold text-foreground">Users</h2>
          <p className="text-muted-foreground mt-2">
            Manage user accounts and permissions
          </p>
        </div>
        <div className="flex items-center space-x-3">
          <button
            onClick={handleRefresh}
            className="inline-flex items-center px-4 py-2 bg-background border border-border text-foreground rounded-md hover:bg-accent transition-colors"
            disabled={isLoading}
          >
            <RefreshCw className={`h-4 w-4 mr-2 ${isLoading ? 'animate-spin' : ''}`} />
            Refresh
          </button>
          <button
            onClick={() => setShowAddDialog(true)}
            className="inline-flex items-center px-4 py-2 bg-primary text-primary-foreground rounded-md hover:bg-primary/90 transition-colors"
          >
            <Plus className="h-4 w-4 mr-2" />
            Add User
          </button>
        </div>
      </div>

      {/* Search */}
      <div className="relative">
        <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 h-4 w-4 text-muted-foreground" />
        <input
          type="text"
          placeholder="Search users by name, email..."
          value={searchQuery}
          onChange={(e) => setSearchQuery(e.target.value)}
          className="w-full pl-10 pr-4 py-2 bg-background border border-border rounded-md text-foreground focus:outline-none focus:ring-2 focus:ring-primary"
        />
      </div>

      {/* Error State */}
      {error && (
        <div className="bg-destructive/10 border border-destructive/20 rounded-lg p-4">
          <p className="text-destructive text-sm">{error.message}</p>
        </div>
      )}

      {/* User List */}
      {isLoading ? (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
          {[...Array(6)].map((_, i) => (
            <div key={i} className="bg-card border border-border rounded-lg p-6 animate-pulse">
              <div className="h-6 bg-muted rounded w-3/4 mb-4"></div>
              <div className="h-4 bg-muted rounded w-full mb-2"></div>
              <div className="h-4 bg-muted rounded w-2/3"></div>
            </div>
          ))}
        </div>
      ) : filteredUsers.length === 0 ? (
        <div className="bg-card border border-border rounded-lg p-12 text-center">
          <UsersIcon className="h-16 w-16 mx-auto text-muted-foreground mb-4" />
          <h3 className="text-2xl font-semibold text-foreground mb-2">
            {searchQuery ? 'No users found' : 'No Users'}
          </h3>
          <p className="text-muted-foreground mb-6">
            {searchQuery 
              ? 'Try adjusting your search query'
              : 'Get started by adding your first user'
            }
          </p>
          {!searchQuery && (
            <button
              onClick={() => setShowAddDialog(true)}
              className="inline-flex items-center px-6 py-3 bg-primary text-primary-foreground rounded-md hover:bg-primary/90 transition-colors"
            >
              <Plus className="h-5 w-5 mr-2" />
              Add User
            </button>
          )}
        </div>
      ) : (
        <>
          <div className="text-sm text-muted-foreground">
            Showing {filteredUsers.length} of {users.length} user{users.length !== 1 ? 's' : ''}
          </div>
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
            {filteredUsers.map((user) => (
              <UserCard
                key={user.user_id}
                user={user}
                onUpdate={refetch}
              />
            ))}
          </div>
        </>
      )}

      {/* Add User Dialog */}
      {showAddDialog && (
        <AddUserDialog
          onClose={() => setShowAddDialog(false)}
          onSuccess={refetch}
        />
      )}
    </div>
  );
}

