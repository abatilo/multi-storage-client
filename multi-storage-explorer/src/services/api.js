/**
 * MSC Explorer API Client
 * Handles all communication with the FastAPI backend
 */
import axios from 'axios';

/**
 * Determine the API base URL based on the environment:
 * 
 * 1. If VITE_API_URL env var is set, use it (explicit override)
 * 2. If running via Vite dev server (npm run dev), use localhost:8888
 * 3. If running as built static files (production), use relative URL (same origin)
 * 
 * This allows the app to work in all scenarios:
 * - Development: Vite on :5175, backend on :8888 (cross-origin with CORS)
 * - Production local: Backend serves static files on :8888 (same origin)
 * - Production cloud: Backend serves static files on any host (same origin)
 */
function getApiBaseUrl() {
  // Explicit override via environment variable
  if (import.meta.env.VITE_API_URL) {
    return import.meta.env.VITE_API_URL;
  }
  
  // Development mode (Vite dev server) - use explicit localhost URL
  // This triggers CORS but backend allows it
  if (import.meta.env.DEV) {
    return 'http://localhost:8888';
  }
  
  // Production build - use relative URL (same origin as where static files are served)
  // This works whether deployed locally, in container, or in cloud
  return '';
}

const API_BASE_URL = getApiBaseUrl();

const api = axios.create({
  baseURL: API_BASE_URL,
  headers: {
    'Content-Type': 'application/json',
  },
});

// Log API URL in development for debugging
if (import.meta.env.DEV) {
  console.log('MSC Explorer API URL:', API_BASE_URL);
}

/**
 * Configuration API
 */
export const uploadConfig = async (file) => {
  const formData = new FormData();
  formData.append('config_file', file);
  
  const response = await api.post('/api/config/upload', formData, {
    headers: {
      'Content-Type': 'multipart/form-data',
    },
  });
  return response.data;
};

export const getProfiles = async () => {
  const response = await api.get('/api/config/profiles');
  return response.data;
};

/**
 * File Operations API
 */
export const listFiles = async (url, options = {}) => {
  const response = await api.post('/api/files/list', {
    url,
    start_after: options.start_after,
    end_at: options.end_at,
    include_directories: options.include_directories !== false,
    limit: options.limit,
  });
  return response.data;
};

export const getFileInfo = async (url) => {
  const response = await api.post('/api/files/info', { url });
  return response.data;
};

export const previewFile = async (url, maxBytes = 1048576) => {
  const response = await api.post('/api/files/preview', { 
    url,
    max_bytes: maxBytes 
  });
  return response.data;
};

export const uploadFile = async (url, file, onProgress) => {
  const formData = new FormData();
  formData.append('url', url);
  formData.append('file', file);
  
  const response = await api.post('/api/files/upload', formData, {
    headers: {
      'Content-Type': 'multipart/form-data',
    },
    onUploadProgress: (progressEvent) => {
      if (onProgress) {
        const percentCompleted = Math.round(
          (progressEvent.loaded * 100) / progressEvent.total
        );
        onProgress(percentCompleted);
      }
    },
  });
  return response.data;
};

export const downloadFile = async (url) => {
  const response = await api.post('/api/files/download', { url }, {
    responseType: 'blob',
  });
  
  // Create a download link
  const downloadUrl = window.URL.createObjectURL(new Blob([response.data]));
  const link = document.createElement('a');
  link.href = downloadUrl;
  link.setAttribute('download', url.split('/').pop() || 'download');
  document.body.appendChild(link);
  link.click();
  link.remove();
  window.URL.revokeObjectURL(downloadUrl);
  
  return { status: 'success', message: 'File downloaded' };
};

export const deleteFile = async (url, recursive = false) => {
  const response = await api.post('/api/files/delete', { url, recursive });
  return response.data;
};

export const copyFile = async (source_url, target_url) => {
  const response = await api.post('/api/files/copy', { source_url, target_url });
  return response.data;
};

export const syncFiles = async (source_url, target_url, options = {}) => {
  const response = await api.post('/api/files/sync', {
    source_url,
    target_url,
    delete_unmatched_files: options.delete_unmatched_files || false,
    preserve_source_attributes: options.preserve_source_attributes || false,
  });
  return response.data;
};

/**
 * Health check
 */
export const healthCheck = async () => {
  const response = await api.get('/api/health');
  return response.data;
};

export default api;


