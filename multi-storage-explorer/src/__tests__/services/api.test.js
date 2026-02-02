/**
 * API Service Tests
 * Tests all API functions with edge cases and error scenarios
 */
import { describe, it, expect, vi } from 'vitest';
import { http, HttpResponse } from 'msw';
import { server } from '../../mocks/server';
import { customHandlers } from '../../mocks/handlers';
import {
  healthCheck,
  getProfiles,
  listFiles,
  getFileInfo,
  previewFile,
  downloadFile,
  deleteFile,
  copyFile,
  syncFiles,
} from '../../services/api';

const BASE = 'http://localhost:8888';

// ============================================================================
// healthCheck() - GET /api/health
// ============================================================================
describe('healthCheck', () => {
  it('returns healthy status', async () => {
    const result = await healthCheck();
    
    expect(result.status).toBe('healthy');
    expect(result).toHaveProperty('config_loaded');
    expect(result).toHaveProperty('frontend_available');
  });

  it('throws on network error', async () => {
    server.use(
      http.get(`${BASE}/api/health`, () => HttpResponse.error())
    );
    
    await expect(healthCheck()).rejects.toThrow();
  });

  it('throws on 500 server error', async () => {
    server.use(
      http.get(`${BASE}/api/health`, () =>
        HttpResponse.json({ detail: 'Internal error' }, { status: 500 })
      )
    );
    
    await expect(healthCheck()).rejects.toMatchObject({
      response: { status: 500 },
    });
  });
});

// ============================================================================
// uploadConfig(file) - POST /api/config/upload
// Note: FormData uploads are tested via ConfigUploader component tests
// as MSW has limitations with FormData in Node.js
// ============================================================================

// ============================================================================
// getProfiles() - GET /api/config/profiles
// ============================================================================
describe('getProfiles', () => {
  it('returns profiles array', async () => {
    const result = await getProfiles();
    
    expect(result.profiles).toEqual(['default-profile', 's3-prod', 'gcs-dev']);
  });

  it('handles empty profiles', async () => {
    server.use(
      http.get(`${BASE}/api/config/profiles`, () =>
        HttpResponse.json({ profiles: [] })
      )
    );
    
    const result = await getProfiles();
    
    expect(result.profiles).toEqual([]);
  });

  it('rejects when no config loaded', async () => {
    server.use(
      http.get(`${BASE}/api/config/profiles`, () =>
        HttpResponse.json({ detail: 'No configuration loaded' }, { status: 404 })
      )
    );
    
    await expect(getProfiles()).rejects.toMatchObject({
      response: { status: 404 },
    });
  });
});

// ============================================================================
// listFiles(url, options) - POST /api/files/list
// ============================================================================
describe('listFiles', () => {
  it('returns files and directories', async () => {
    const result = await listFiles('msc://profile/path/');
    
    expect(result.items).toHaveLength(4);
    // Backend removes trailing slash from directory names
    expect(result.items[0]).toMatchObject({ name: 'documents', is_directory: true, key: 'documents/' });
    expect(result.items[2]).toMatchObject({ name: 'readme.txt', is_directory: false });
  });

  it('handles empty directory', async () => {
    server.use(customHandlers.emptyDirectory());
    
    const result = await listFiles('msc://profile/empty/');
    
    expect(result.items).toEqual([]);
  });

  it('sends correct payload with default options', async () => {
    let capturedBody = null;
    server.use(
      http.post(`${BASE}/api/files/list`, async ({ request }) => {
        capturedBody = await request.json();
        return HttpResponse.json({ objects: [] });
      })
    );
    
    await listFiles('msc://profile/path/');
    
    expect(capturedBody).toMatchObject({
      url: 'msc://profile/path/',
      include_directories: true,
    });
  });

  it('sends pagination limit when provided', async () => {
    let capturedBody = null;
    server.use(
      http.post(`${BASE}/api/files/list`, async ({ request }) => {
        capturedBody = await request.json();
        return HttpResponse.json({ objects: [] });
      })
    );
    
    await listFiles('msc://profile/path/', { limit: 50 });
    
    expect(capturedBody.limit).toBe(50);
  });

  it('sends pagination cursor when provided', async () => {
    let capturedBody = null;
    server.use(
      http.post(`${BASE}/api/files/list`, async ({ request }) => {
        capturedBody = await request.json();
        return HttpResponse.json({ objects: [] });
      })
    );
    
    await listFiles('msc://profile/path/', { start_after: 'file99.txt', limit: 100 });
    
    expect(capturedBody.start_after).toBe('file99.txt');
    expect(capturedBody.limit).toBe(100);
  });

  it('sends include_directories=false when specified', async () => {
    let capturedBody = null;
    server.use(
      http.post(`${BASE}/api/files/list`, async ({ request }) => {
        capturedBody = await request.json();
        return HttpResponse.json({ objects: [] });
      })
    );
    
    await listFiles('msc://profile/path/', { include_directories: false });
    
    expect(capturedBody.include_directories).toBe(false);
  });

  it('rejects on invalid profile', async () => {
    server.use(
      http.post(`${BASE}/api/files/list`, () =>
        HttpResponse.json({ detail: 'Profile not found' }, { status: 404 })
      )
    );
    
    await expect(listFiles('msc://invalid/path/')).rejects.toMatchObject({
      response: { status: 404 },
    });
  });

  it('rejects on permission denied', async () => {
    server.use(
      http.post(`${BASE}/api/files/list`, () =>
        HttpResponse.json({ detail: 'Permission denied' }, { status: 403 })
      )
    );
    
    await expect(listFiles('msc://restricted/')).rejects.toMatchObject({
      response: { status: 403 },
    });
  });

  it('rejects on malformed URL', async () => {
    server.use(
      http.post(`${BASE}/api/files/list`, () =>
        HttpResponse.json({ detail: 'Invalid MSC URL' }, { status: 400 })
      )
    );
    
    await expect(listFiles('invalid-url')).rejects.toMatchObject({
      response: { status: 400 },
    });
  });
});

// ============================================================================
// getFileInfo(url) - POST /api/files/info
// ============================================================================
describe('getFileInfo', () => {
  it('returns file metadata', async () => {
    const result = await getFileInfo('msc://profile/file.txt');
    
    // Backend returns ObjectMetadata format with key and content_length
    expect(result).toMatchObject({
      key: 'file.txt',
      type: 'file',
      content_length: 1024,
    });
  });

  it('returns directory metadata', async () => {
    server.use(
      http.post(`${BASE}/api/files/info`, () =>
        HttpResponse.json({
          key: 'folder/',
          type: 'directory',
          content_length: 0,
        })
      )
    );
    
    const result = await getFileInfo('msc://profile/folder/');
    
    expect(result).toMatchObject({
      key: 'folder/',
      type: 'directory',
    });
  });

  it('rejects on file not found', async () => {
    server.use(
      http.post(`${BASE}/api/files/info`, () =>
        HttpResponse.json({ detail: 'File not found' }, { status: 404 })
      )
    );
    
    await expect(getFileInfo('msc://profile/missing.txt')).rejects.toMatchObject({
      response: { status: 404 },
    });
  });

  it('rejects on permission denied', async () => {
    server.use(
      http.post(`${BASE}/api/files/info`, () =>
        HttpResponse.json({ detail: 'Permission denied' }, { status: 403 })
      )
    );
    
    await expect(getFileInfo('msc://restricted/file.txt')).rejects.toMatchObject({
      response: { status: 403 },
    });
  });
});

// ============================================================================
// previewFile(url, maxBytes) - POST /api/files/preview
// ============================================================================
describe('previewFile', () => {
  it('returns text file preview', async () => {
    const result = await previewFile('msc://profile/readme.txt');
    
    expect(result).toMatchObject({
      can_preview: true,
      is_text: true,
      is_image: false,
      content_truncated: false,
    });
    expect(result.content).toContain('Sample file content');
  });

  it('returns image file preview', async () => {
    server.use(customHandlers.previewImage());
    
    const result = await previewFile('msc://profile/image.png');
    
    expect(result).toMatchObject({
      can_preview: true,
      is_text: false,
      is_image: true,
    });
    expect(result.content).toBeTruthy();
  });

  it('handles large file with truncated content', async () => {
    server.use(customHandlers.previewTruncated());
    
    const result = await previewFile('msc://profile/large-file.txt');
    
    expect(result.content_truncated).toBe(true);
    expect(result.file_info.size).toBe(10485760);
  });

  it('returns can_preview=false for binary files', async () => {
    server.use(customHandlers.previewBinary());
    
    const result = await previewFile('msc://profile/binary.exe');
    
    expect(result.can_preview).toBe(false);
    expect(result.content).toBeNull();
    expect(result.preview_message).toContain('Binary file');
  });

  it('sends max_bytes parameter', async () => {
    let capturedBody = null;
    server.use(
      http.post(`${BASE}/api/files/preview`, async ({ request }) => {
        capturedBody = await request.json();
        return HttpResponse.json({
          can_preview: true,
          is_text: true,
          content: 'test',
          file_info: {},
        });
      })
    );
    
    await previewFile('msc://profile/file.txt', 512000);
    
    expect(capturedBody.max_bytes).toBe(512000);
  });

  it('uses default max_bytes of 1MB', async () => {
    let capturedBody = null;
    server.use(
      http.post(`${BASE}/api/files/preview`, async ({ request }) => {
        capturedBody = await request.json();
        return HttpResponse.json({
          can_preview: true,
          is_text: true,
          content: 'test',
          file_info: {},
        });
      })
    );
    
    await previewFile('msc://profile/file.txt');
    
    expect(capturedBody.max_bytes).toBe(1048576);
  });

  it('rejects on file not found', async () => {
    server.use(
      http.post(`${BASE}/api/files/preview`, () =>
        HttpResponse.json({ detail: 'File not found' }, { status: 404 })
      )
    );
    
    await expect(previewFile('msc://profile/missing.txt')).rejects.toMatchObject({
      response: { status: 404 },
    });
  });
});

// ============================================================================
// uploadFile(url, file, onProgress) - POST /api/files/upload
// Note: FormData uploads are tested via FileManager component tests
// as MSW has limitations with FormData in Node.js
// ============================================================================

// ============================================================================
// downloadFile(url) - POST /api/files/download
// ============================================================================
describe('downloadFile', () => {
  it('triggers file download', async () => {
    const mockLink = {
      href: '',
      setAttribute: vi.fn(),
      click: vi.fn(),
      remove: vi.fn(),
    };
    const originalCreateElement = document.createElement.bind(document);
    vi.spyOn(document, 'createElement').mockImplementation((tag) => {
      if (tag === 'a') return mockLink;
      return originalCreateElement(tag);
    });
    vi.spyOn(document.body, 'appendChild').mockImplementation(() => {});
    
    const result = await downloadFile('msc://profile/file.txt');
    
    expect(result.status).toBe('success');
    expect(mockLink.click).toHaveBeenCalled();
    expect(mockLink.setAttribute).toHaveBeenCalledWith('download', 'file.txt');
    
    vi.restoreAllMocks();
  });

  it('rejects on file not found', async () => {
    server.use(
      http.post(`${BASE}/api/files/download`, () =>
        HttpResponse.json({ detail: 'File not found' }, { status: 404 })
      )
    );
    
    await expect(downloadFile('msc://profile/missing.txt')).rejects.toMatchObject({
      response: { status: 404 },
    });
  });

  it('rejects on permission denied', async () => {
    server.use(
      http.post(`${BASE}/api/files/download`, () =>
        HttpResponse.json({ detail: 'Permission denied' }, { status: 403 })
      )
    );
    
    await expect(downloadFile('msc://restricted/file.txt')).rejects.toMatchObject({
      response: { status: 403 },
    });
  });
});

// ============================================================================
// deleteFile(url, recursive) - POST /api/files/delete
// ============================================================================
describe('deleteFile', () => {
  it('deletes file successfully', async () => {
    const result = await deleteFile('msc://profile/file.txt');
    
    expect(result.status).toBe('success');
  });

  it('sends recursive=false by default', async () => {
    let capturedBody = null;
    server.use(
      http.post(`${BASE}/api/files/delete`, async ({ request }) => {
        capturedBody = await request.json();
        return HttpResponse.json({ status: 'success' });
      })
    );
    
    await deleteFile('msc://profile/file.txt');
    
    expect(capturedBody.recursive).toBe(false);
  });

  it('sends recursive=true when specified', async () => {
    let capturedBody = null;
    server.use(
      http.post(`${BASE}/api/files/delete`, async ({ request }) => {
        capturedBody = await request.json();
        return HttpResponse.json({ status: 'success' });
      })
    );
    
    await deleteFile('msc://profile/folder/', true);
    
    expect(capturedBody.recursive).toBe(true);
  });

  it('rejects on directory not empty (non-recursive)', async () => {
    server.use(
      http.post(`${BASE}/api/files/delete`, () =>
        HttpResponse.json({ detail: 'Directory not empty' }, { status: 400 })
      )
    );
    
    await expect(deleteFile('msc://profile/folder/')).rejects.toMatchObject({
      response: { status: 400 },
    });
  });

  it('rejects on file not found', async () => {
    server.use(
      http.post(`${BASE}/api/files/delete`, () =>
        HttpResponse.json({ detail: 'File not found' }, { status: 404 })
      )
    );
    
    await expect(deleteFile('msc://profile/missing.txt')).rejects.toMatchObject({
      response: { status: 404 },
    });
  });

  it('rejects on permission denied', async () => {
    server.use(
      http.post(`${BASE}/api/files/delete`, () =>
        HttpResponse.json({ detail: 'Permission denied' }, { status: 403 })
      )
    );
    
    await expect(deleteFile('msc://restricted/file.txt')).rejects.toMatchObject({
      response: { status: 403 },
    });
  });
});

// ============================================================================
// copyFile(source_url, target_url) - POST /api/files/copy
// ============================================================================
describe('copyFile', () => {
  it('copies file successfully', async () => {
    const result = await copyFile('msc://profile/source.txt', 'msc://profile/target.txt');
    
    expect(result.status).toBe('success');
  });

  it('sends correct payload', async () => {
    let capturedBody = null;
    server.use(
      http.post(`${BASE}/api/files/copy`, async ({ request }) => {
        capturedBody = await request.json();
        return HttpResponse.json({ status: 'success' });
      })
    );
    
    await copyFile('msc://profile/source.txt', 'msc://profile/dest.txt');
    
    expect(capturedBody).toEqual({
      source_url: 'msc://profile/source.txt',
      target_url: 'msc://profile/dest.txt',
    });
  });

  it('rejects on source not found', async () => {
    server.use(
      http.post(`${BASE}/api/files/copy`, () =>
        HttpResponse.json({ detail: 'Source not found' }, { status: 404 })
      )
    );
    
    await expect(
      copyFile('msc://profile/missing.txt', 'msc://profile/dest.txt')
    ).rejects.toMatchObject({
      response: { status: 404 },
    });
  });

  it('rejects on target already exists', async () => {
    server.use(
      http.post(`${BASE}/api/files/copy`, () =>
        HttpResponse.json({ detail: 'Target already exists' }, { status: 409 })
      )
    );
    
    await expect(
      copyFile('msc://profile/source.txt', 'msc://profile/existing.txt')
    ).rejects.toMatchObject({
      response: { status: 409 },
    });
  });

  it('rejects on cross-profile copy', async () => {
    server.use(
      http.post(`${BASE}/api/files/copy`, () =>
        HttpResponse.json({ detail: 'Cannot copy across profiles' }, { status: 400 })
      )
    );
    
    await expect(
      copyFile('msc://profile1/file.txt', 'msc://profile2/file.txt')
    ).rejects.toMatchObject({
      response: { status: 400 },
    });
  });

  it('rejects on permission denied', async () => {
    server.use(
      http.post(`${BASE}/api/files/copy`, () =>
        HttpResponse.json({ detail: 'Permission denied' }, { status: 403 })
      )
    );
    
    await expect(
      copyFile('msc://restricted/file.txt', 'msc://profile/dest.txt')
    ).rejects.toMatchObject({
      response: { status: 403 },
    });
  });
});

// ============================================================================
// syncFiles(source_url, target_url, options) - POST /api/files/sync
// ============================================================================
describe('syncFiles', () => {
  it('syncs files successfully', async () => {
    const result = await syncFiles('msc://profile/source/', 'msc://profile/target/');
    
    expect(result.status).toBe('success');
    expect(result.files_synced).toBe(10);
  });

  it('sends default options as false', async () => {
    let capturedBody = null;
    server.use(
      http.post(`${BASE}/api/files/sync`, async ({ request }) => {
        capturedBody = await request.json();
        return HttpResponse.json({ status: 'success', files_synced: 0 });
      })
    );
    
    await syncFiles('msc://profile/source/', 'msc://profile/target/');
    
    expect(capturedBody).toMatchObject({
      source_url: 'msc://profile/source/',
      target_url: 'msc://profile/target/',
      delete_unmatched_files: false,
      preserve_source_attributes: false,
    });
  });

  it('sends delete_unmatched_files=true when specified', async () => {
    let capturedBody = null;
    server.use(
      http.post(`${BASE}/api/files/sync`, async ({ request }) => {
        capturedBody = await request.json();
        return HttpResponse.json({ status: 'success', files_synced: 0 });
      })
    );
    
    await syncFiles('msc://profile/source/', 'msc://profile/target/', {
      delete_unmatched_files: true,
    });
    
    expect(capturedBody.delete_unmatched_files).toBe(true);
  });

  it('sends preserve_source_attributes=true when specified', async () => {
    let capturedBody = null;
    server.use(
      http.post(`${BASE}/api/files/sync`, async ({ request }) => {
        capturedBody = await request.json();
        return HttpResponse.json({ status: 'success', files_synced: 0 });
      })
    );
    
    await syncFiles('msc://profile/source/', 'msc://profile/target/', {
      preserve_source_attributes: true,
    });
    
    expect(capturedBody.preserve_source_attributes).toBe(true);
  });

  it('rejects on source not found', async () => {
    server.use(
      http.post(`${BASE}/api/files/sync`, () =>
        HttpResponse.json({ detail: 'Source not found' }, { status: 404 })
      )
    );
    
    await expect(
      syncFiles('msc://profile/missing/', 'msc://profile/target/')
    ).rejects.toMatchObject({
      response: { status: 404 },
    });
  });

  it('rejects on permission denied', async () => {
    server.use(
      http.post(`${BASE}/api/files/sync`, () =>
        HttpResponse.json({ detail: 'Permission denied' }, { status: 403 })
      )
    );
    
    await expect(
      syncFiles('msc://restricted/', 'msc://profile/target/')
    ).rejects.toMatchObject({
      response: { status: 403 },
    });
  });
});
