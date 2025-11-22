/**
 * Visual test/demo for CellValue component
 * This demonstrates how the component handles different data types
 */

import { CellValue } from '../CellValue';

export default function CellValueDemo() {
  // Test data matching the MongoDB example from the user
  const testData = [
    {
      label: 'String (short)',
      value: 'Mozilla/5.0',
      dataType: undefined,
    },
    {
      label: 'String (long text)',
      value: 'This is a very long text string that exceeds the maximum length threshold and should be truncated with an expand button to view the full content. It contains multiple sentences and demonstrates how the component handles lengthy text fields that would otherwise break the table layout.',
      dataType: 'text',
    },
    {
      label: 'Number',
      value: 1483.77,
      dataType: undefined,
    },
    {
      label: 'Boolean (true)',
      value: true,
      dataType: 'boolean',
    },
    {
      label: 'Boolean (false)',
      value: false,
      dataType: 'boolean',
    },
    {
      label: 'Boolean (string "true")',
      value: 'true',
      dataType: 'boolean',
    },
    {
      label: 'Boolean (string "false")',
      value: 'false',
      dataType: 'boolean',
    },
    {
      label: 'Null',
      value: null,
      dataType: undefined,
    },
    {
      label: 'Undefined',
      value: undefined,
      dataType: undefined,
    },
    {
      label: 'Object (nested)',
      value: {
        browser: 'Chrome',
        device: 'desktop',
        os: 'Windows',
      },
      dataType: undefined,
    },
    {
      label: 'Object (complex)',
      value: {
        paymentProcessor: 'stripe',
        transactionId: 'txn_123456',
      },
      dataType: undefined,
    },
    {
      label: 'Array of strings',
      value: ['item1', 'item2', 'item3'],
      dataType: undefined,
    },
    {
      label: 'Array of objects',
      value: [
        { name: 'Alice', age: 30 },
        { name: 'Bob', age: 25 },
      ],
      dataType: undefined,
    },
    {
      label: 'Empty object',
      value: {},
      dataType: undefined,
    },
    {
      label: 'Empty array',
      value: [],
      dataType: undefined,
    },
    {
      label: 'JSONB (stringified)',
      value: '{"difficulty": "easy", "source_type": "QA pair"}',
      dataType: 'jsonb',
    },
    {
      label: 'Array (integer[])',
      value: '{1,2,3,4,5}',
      dataType: 'integer[]',
    },
    {
      label: 'Array (text[])',
      value: '{apple,banana,cherry}',
      dataType: 'text[]',
    },
    {
      label: 'Array (varchar[])',
      value: '{"first item","second item","third item"}',
      dataType: 'varchar[]',
    },
    {
      label: 'Array (native)',
      value: ['red', 'green', 'blue'],
      dataType: undefined,
    },
    {
      label: 'Date',
      value: '2024-08-07',
      dataType: 'date',
    },
    {
      label: 'Timestamp (no timezone)',
      value: '2024-08-07 17:11:58.921215',
      dataType: 'timestamp without time zone',
    },
    {
      label: 'Timestamp (with timezone)',
      value: '2023-11-20T08:00:00Z',
      dataType: 'timestamp with time zone',
    },
    {
      label: 'DateTime',
      value: '2024-08-07 17:11:58',
      dataType: 'datetime',
    },
    {
      label: 'Time',
      value: '17:11:58',
      dataType: 'time',
    },
  ];

  return (
    <div className="p-8 space-y-4">
      <h1 className="text-2xl font-bold">CellValue Component Test</h1>
      <p className="text-muted-foreground">
        This demonstrates how different data types are rendered
      </p>

      <table className="w-full border border-border">
        <thead>
          <tr className="bg-muted">
            <th className="border border-border px-4 py-2 text-left">Type</th>
            <th className="border border-border px-4 py-2 text-left">Data Type</th>
            <th className="border border-border px-4 py-2 text-left">Rendered Value</th>
          </tr>
        </thead>
        <tbody>
          {testData.map((item, index) => (
            <tr key={index} className="border-b border-border">
              <td className="border border-border px-4 py-2 font-semibold">
                {item.label}
              </td>
              <td className="border border-border px-4 py-2 text-xs font-mono text-muted-foreground">
                {item.dataType || 'auto'}
              </td>
              <td className="border border-border px-4 py-2">
                <CellValue value={item.value} dataType={item.dataType} />
              </td>
            </tr>
          ))}
        </tbody>
      </table>

      <div className="mt-8 p-4 bg-muted rounded">
        <h2 className="font-semibold mb-2">Expected Behavior:</h2>
        <ul className="list-disc list-inside space-y-1 text-sm">
          <li>Short strings display normally</li>
          <li>Long strings (100+ chars) are truncated with expand button</li>
          <li>Character count badge shows total length for long strings</li>
          <li>Numbers display as-is</li>
          <li>Booleans show with visual checkmark/X icons and colored text</li>
          <li>NULL/undefined display in italic with muted color</li>
          <li>Objects show a count badge and preview (e.g., "3 keys")</li>
          <li>Arrays show a count badge and preview (e.g., "3 items")</li>
          <li>PostgreSQL array strings (e.g., {'{'}1,2,3{'}'}) are automatically parsed</li>
          <li>Click the expand icon to view full content</li>
          <li>Copy button allows copying full text/JSON to clipboard</li>
          <li>JSONB strings are automatically parsed and displayed as objects</li>
          <li>Dates/times are formatted for easy reading with relative time tooltip</li>
        </ul>
      </div>
    </div>
  );
}

