interface SettingsPageProps {
  params: Promise<{
    tenant: string;
  }>;
}

export default async function SettingsPage({ params }: SettingsPageProps) {
  const { tenant } = await params;

  return (
    <div className="space-y-6">
      <div>
        <h2 className="text-3xl font-bold text-foreground">Settings</h2>
        <p className="text-muted-foreground mt-2">
          Manage your profile, tenant settings, and preferences.
        </p>
      </div>

      <div className="bg-card rounded-lg border border-border p-8 text-center">
        <h3 className="text-lg font-semibold text-foreground mb-2">Settings & Preferences</h3>
        <p className="text-muted-foreground mb-4">
          This section will contain user profile settings, tenant configuration, API keys, and security settings.
        </p>
        <div className="text-sm text-muted-foreground">
          Coming soon...
        </div>
      </div>
    </div>
  );
}

export async function generateMetadata({ params }: SettingsPageProps) {
  const { tenant } = await params;
  
  return {
    title: `Settings | ${tenant} | reDB`,
    description: `Settings and preferences for ${tenant}`,
  };
}
