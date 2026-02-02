/**
 * MSW Request Handlers
 * Mock API responses for testing
 */
import { http, HttpResponse } from 'msw';

const BASE = 'http://localhost:8888';

// Default success responses for happy path testing
export const handlers = [
  // Health check
  http.get(`${BASE}/api/health`, () => {
    return HttpResponse.json({
      status: 'healthy',
      config_loaded: false,
      frontend_available: true,
    });
  }),

  // Config upload
  http.post(`${BASE}/api/config/upload`, () => {
    return HttpResponse.json({
      profiles: ['default-profile', 's3-prod', 'gcs-dev'],
    });
  }),

  // Get profiles
  http.get(`${BASE}/api/config/profiles`, () => {
    return HttpResponse.json({
      profiles: ['default-profile', 's3-prod', 'gcs-dev'],
    });
  }),

  // List files
  http.post(`${BASE}/api/files/list`, () => {
    return HttpResponse.json({
      items: [
        { 
          name: 'documents', 
          key: 'documents/',
          is_directory: true, 
          type: 'directory',
          size: 0, 
          last_modified: '2024-01-15T10:30:00Z'
        },
        { 
          name: 'images', 
          key: 'images/',
          is_directory: true, 
          type: 'directory',
          size: 0, 
          last_modified: '2024-01-15T10:30:00Z'
        },
        { 
          name: 'readme.txt', 
          key: 'readme.txt',
          is_directory: false, 
          type: 'file',
          size: 1024, 
          last_modified: '2024-01-15T10:30:00Z'
        },
        { 
          name: 'data.json', 
          key: 'data.json',
          is_directory: false, 
          type: 'file',
          size: 2048, 
          last_modified: '2024-01-14T15:20:00Z'
        },
      ],
      count: 4
    });
  }),

  // File info
  http.post(`${BASE}/api/files/info`, async ({ request }) => {
    const body = await request.json();
    const fileName = body.url.split('/').pop() || 'unknown';
    return HttpResponse.json({
      key: fileName,
      content_length: 1024,
      last_modified: '2024-01-15T10:30:00Z',
      type: 'file',
      content_type: 'text/plain',
      etag: '"abc123"',
    });
  }),

  // File preview
  http.post(`${BASE}/api/files/preview`, () => {
    return HttpResponse.json({
      can_preview: true,
      is_text: true,
      is_image: false,
      content: 'Sample file content for preview.\nLine 2 of the file.\nLine 3 of the file.',
      content_truncated: false,
      file_info: {
        name: 'readme.txt',
        size: 1024,
        type: 'file',
        content_type: 'text/plain',
        last_modified: '2024-01-15T10:30:00Z',
      },
    });
  }),

  // File upload
  http.post(`${BASE}/api/files/upload`, () => {
    return HttpResponse.json({
      status: 'success',
      url: 'msc://profile/uploaded-file.txt',
    });
  }),

  // File download - returns blob
  http.post(`${BASE}/api/files/download`, () => {
    return new HttpResponse('file content here', {
      headers: {
        'Content-Type': 'application/octet-stream',
      },
    });
  }),

  // File delete
  http.post(`${BASE}/api/files/delete`, () => {
    return HttpResponse.json({
      status: 'success',
    });
  }),

  // File copy
  http.post(`${BASE}/api/files/copy`, () => {
    return HttpResponse.json({
      status: 'success',
    });
  }),

  // File sync
  http.post(`${BASE}/api/files/sync`, () => {
    return HttpResponse.json({
      status: 'success',
      files_synced: 10,
    });
  }),
];

/**
 * Error handler factories for testing error scenarios
 * Usage: server.use(errorHandlers.notFound('/api/files/list'))
 */
export const errorHandlers = {
  // 404 Not Found
  notFound: (path, detail = 'Not found') =>
    http.post(`${BASE}${path}`, () =>
      HttpResponse.json({ detail }, { status: 404 })
    ),

  notFoundGet: (path, detail = 'Not found') =>
    http.get(`${BASE}${path}`, () =>
      HttpResponse.json({ detail }, { status: 404 })
    ),

  // 403 Forbidden
  forbidden: (path, detail = 'Permission denied') =>
    http.post(`${BASE}${path}`, () =>
      HttpResponse.json({ detail }, { status: 403 })
    ),

  // 400 Bad Request
  badRequest: (path, detail = 'Invalid request') =>
    http.post(`${BASE}${path}`, () =>
      HttpResponse.json({ detail }, { status: 400 })
    ),

  // 409 Conflict
  conflict: (path, detail = 'Resource conflict') =>
    http.post(`${BASE}${path}`, () =>
      HttpResponse.json({ detail }, { status: 409 })
    ),

  // 413 Payload Too Large
  payloadTooLarge: (path, detail = 'File too large') =>
    http.post(`${BASE}${path}`, () =>
      HttpResponse.json({ detail }, { status: 413 })
    ),

  // 500 Internal Server Error
  serverError: (path, detail = 'Internal server error') =>
    http.post(`${BASE}${path}`, () =>
      HttpResponse.json({ detail }, { status: 500 })
    ),

  // 507 Insufficient Storage
  insufficientStorage: (path, detail = 'Storage quota exceeded') =>
    http.post(`${BASE}${path}`, () =>
      HttpResponse.json({ detail }, { status: 507 })
    ),

  // Network error (connection failed)
  networkError: (path) =>
    http.post(`${BASE}${path}`, () => HttpResponse.error()),

  networkErrorGet: (path) =>
    http.get(`${BASE}${path}`, () => HttpResponse.error()),
};

/**
 * Custom response factories for specific test scenarios
 */
export const customHandlers = {
  // Empty profiles list
  emptyProfiles: () =>
    http.post(`${BASE}/api/config/upload`, () =>
      HttpResponse.json({ profiles: [] })
    ),

  // Empty directory listing
  emptyDirectory: () =>
    http.post(`${BASE}/api/files/list`, () =>
      HttpResponse.json({ items: [] })
    ),

  // Preview with truncated content
  previewTruncated: () =>
    http.post(`${BASE}/api/files/preview`, () =>
      HttpResponse.json({
        can_preview: true,
        is_text: true,
        is_image: false,
        content: 'Truncated content...',
        content_truncated: true,
        file_info: {
          name: 'large-file.txt',
          size: 10485760, // 10MB
          type: 'file',
          content_type: 'text/plain',
        },
      })
    ),

  // Preview not available (binary file)
  previewBinary: () =>
    http.post(`${BASE}/api/files/preview`, () =>
      HttpResponse.json({
        can_preview: false,
        is_text: false,
        is_image: false,
        content: null,
        preview_message: 'Binary file cannot be previewed',
        file_info: {
          name: 'binary.exe',
          size: 1024,
          type: 'file',
          content_type: 'application/octet-stream',
        },
      })
    ),

  // Image preview
  previewImage: () =>
    http.post(`${BASE}/api/files/preview`, () =>
      HttpResponse.json({
        can_preview: true,
        is_text: false,
        is_image: true,
        content: 'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg==',
        content_truncated: false,
        file_info: {
          name: 'image.png',
          size: 2048,
          type: 'file',
          content_type: 'image/png',
        },
      })
    ),

  // Capture request body for assertions
  captureRequest: (path, callback) =>
    http.post(`${BASE}${path}`, async ({ request }) => {
      const body = await request.json();
      callback(body);
      return HttpResponse.json({ status: 'success' });
    }),
};

