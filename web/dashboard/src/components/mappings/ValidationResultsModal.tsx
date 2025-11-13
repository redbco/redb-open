'use client';

import { X, CheckCircle, AlertCircle, AlertTriangle } from 'lucide-react';

interface ValidationResultsModalProps {
  results: {
    is_valid: boolean;
    errors: string[];
    warnings: string[];
    validated_at: string;
  };
  onClose: () => void;
}

export function ValidationResultsModal({ results, onClose }: ValidationResultsModalProps) {
  const hasErrors = results.errors && results.errors.length > 0;
  const hasWarnings = results.warnings && results.warnings.length > 0;

  return (
    <>
      {/* Backdrop */}
      <div
        className="fixed inset-0 bg-black/50 z-40"
        onClick={onClose}
      />

      {/* Modal */}
      <div className="fixed inset-0 z-50 flex items-center justify-center p-4">
        <div
          className="bg-card border border-border rounded-lg shadow-xl max-w-2xl w-full max-h-[80vh] overflow-hidden flex flex-col"
          onClick={(e) => e.stopPropagation()}
        >
          {/* Header */}
          <div className="flex items-center justify-between p-6 border-b border-border">
            <div className="flex items-center gap-3">
              {results.is_valid ? (
                <CheckCircle className="h-6 w-6 text-green-600 dark:text-green-400" />
              ) : (
                <AlertCircle className="h-6 w-6 text-red-600 dark:text-red-400" />
              )}
              <h2 className="text-xl font-semibold text-foreground">
                Mapping Validation Results
              </h2>
            </div>
            <button
              onClick={onClose}
              className="p-1 rounded-md hover:bg-accent text-muted-foreground hover:text-foreground transition-colors"
            >
              <X className="h-5 w-5" />
            </button>
          </div>

          {/* Content */}
          <div className="flex-1 overflow-y-auto p-6 space-y-4">
            {/* Status Badge */}
            <div className={`inline-flex items-center px-4 py-2 rounded-lg ${
              results.is_valid 
                ? 'bg-green-100 dark:bg-green-900/30 border border-green-200 dark:border-green-800/30'
                : 'bg-red-100 dark:bg-red-900/30 border border-red-200 dark:border-red-800/30'
            }`}>
              <span className={`text-sm font-semibold ${
                results.is_valid
                  ? 'text-green-800 dark:text-green-200'
                  : 'text-red-800 dark:text-red-200'
              }`}>
                {results.is_valid ? 'Validation Passed' : 'Validation Failed'}
              </span>
            </div>

            {/* Timestamp */}
            <div className="text-sm text-muted-foreground">
              Validated at: {new Date(results.validated_at).toLocaleString()}
            </div>

            {/* Errors Section */}
            {hasErrors && (
              <div className="bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800/30 rounded-lg p-4">
                <div className="flex items-center gap-2 mb-3">
                  <AlertCircle className="h-5 w-5 text-red-600 dark:text-red-400" />
                  <h3 className="text-sm font-semibold text-red-800 dark:text-red-200">
                    Errors ({results.errors.length})
                  </h3>
                </div>
                <ul className="space-y-2">
                  {results.errors.map((error, idx) => (
                    <li key={idx} className="text-sm text-red-700 dark:text-red-300 flex items-start gap-2">
                      <span className="inline-block w-1.5 h-1.5 rounded-full bg-red-600 dark:bg-red-400 mt-1.5 flex-shrink-0" />
                      <span>{error}</span>
                    </li>
                  ))}
                </ul>
              </div>
            )}

            {/* Warnings Section */}
            {hasWarnings && (
              <div className="bg-yellow-50 dark:bg-yellow-900/20 border border-yellow-200 dark:border-yellow-800/30 rounded-lg p-4">
                <div className="flex items-center gap-2 mb-3">
                  <AlertTriangle className="h-5 w-5 text-yellow-600 dark:text-yellow-400" />
                  <h3 className="text-sm font-semibold text-yellow-800 dark:text-yellow-200">
                    Warnings ({results.warnings.length})
                  </h3>
                </div>
                <ul className="space-y-2">
                  {results.warnings.map((warning, idx) => (
                    <li key={idx} className="text-sm text-yellow-700 dark:text-yellow-300 flex items-start gap-2">
                      <span className="inline-block w-1.5 h-1.5 rounded-full bg-yellow-600 dark:bg-yellow-400 mt-1.5 flex-shrink-0" />
                      <span>{warning}</span>
                    </li>
                  ))}
                </ul>
              </div>
            )}

            {/* Success Message */}
            {!hasErrors && !hasWarnings && results.is_valid && (
              <div className="bg-green-50 dark:bg-green-900/20 border border-green-200 dark:border-green-800/30 rounded-lg p-4">
                <div className="flex items-center gap-2">
                  <CheckCircle className="h-5 w-5 text-green-600 dark:text-green-400" />
                  <p className="text-sm text-green-800 dark:text-green-200">
                    All validation checks passed successfully. This mapping is ready to use.
                  </p>
                </div>
              </div>
            )}
          </div>

          {/* Footer */}
          <div className="flex items-center justify-end gap-3 p-6 border-t border-border">
            <button
              onClick={onClose}
              className="px-4 py-2 bg-primary text-primary-foreground rounded-md hover:bg-primary/90 transition-colors"
            >
              Close
            </button>
          </div>
        </div>
      </div>
    </>
  );
}

