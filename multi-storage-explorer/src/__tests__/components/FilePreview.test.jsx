/**
 * FilePreview Component Tests
 * Tests file metadata modal functionality
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, waitFor } from '@testing-library/react';
import { http, HttpResponse } from 'msw';
import { server } from '../../mocks/server';
import FilePreview from '../../components/FilePreview';

const BASE = 'http://localhost:8888';

describe('FilePreview', () => {
  const defaultProps = {
    visible: true,
    fileUrl: 'msc://profile/test-file.txt',
    fileName: 'test-file.txt',
    onClose: vi.fn(),
  };

  beforeEach(() => {
    vi.clearAllMocks();
  });

  describe('Rendering', () => {
    it('renders modal with file name', async () => {
      server.use(
        http.post(`${BASE}/api/files/info`, () =>
          HttpResponse.json({
            key: 'test-file.txt',
            content_length: 1024,
            last_modified: '2024-01-15T10:30:00Z',
            type: 'file',
            content_type: 'text/plain',
            etag: '"abc123"',
          })
        )
      );

      render(<FilePreview {...defaultProps} />);

      await waitFor(() => {
        expect(screen.getByText('test-file.txt')).toBeInTheDocument();
      });
    });

    it('shows loading spinner while fetching metadata', async () => {
      server.use(
        http.post(`${BASE}/api/files/info`, async () => {
          await new Promise((resolve) => setTimeout(resolve, 100));
          return HttpResponse.json({
            key: 'test.txt',
            content_length: 100,
            last_modified: '2024-01-15T10:30:00Z',
            type: 'file',
            content_type: 'text/plain',
          });
        })
      );

      render(<FilePreview {...defaultProps} />);

      expect(screen.getByText(/loading metadata/i)).toBeInTheDocument();
    });

    it('does not render when visible is false', () => {
      render(<FilePreview {...defaultProps} visible={false} />);

      expect(screen.queryByText('test-file.txt')).not.toBeInTheDocument();
    });
  });

  describe('Error Handling', () => {
    it('shows error when file not found', async () => {
      server.use(
        http.post(`${BASE}/api/files/info`, () =>
          HttpResponse.json({ detail: 'File not found' }, { status: 404 })
        )
      );

      render(<FilePreview {...defaultProps} />);

      await waitFor(() => {
        expect(screen.getByText(/error/i)).toBeInTheDocument();
      });
    });

    it('shows error on permission denied', async () => {
      server.use(
        http.post(`${BASE}/api/files/info`, () =>
          HttpResponse.json({ detail: 'Permission denied' }, { status: 403 })
        )
      );

      render(<FilePreview {...defaultProps} />);

      await waitFor(() => {
        expect(screen.getByText(/error/i)).toBeInTheDocument();
      });
    });
  });

  describe('Metadata Display', () => {
    it('shows file metadata in metadata tab', async () => {
      server.use(
        http.post(`${BASE}/api/files/info`, () =>
          HttpResponse.json({
            key: 'metadata-test.txt',
            content_length: 1024,
            last_modified: '2024-01-15T10:30:00Z',
            type: 'file',
            content_type: 'text/plain',
            etag: '"abc123"',
          })
        )
      );

      render(<FilePreview {...defaultProps} fileName="metadata-test.txt" />);

      await waitFor(() => {
        expect(screen.getByRole('tab', { name: /metadata/i })).toBeInTheDocument();
      });

      await waitFor(() => {
        // Check for metadata content
        expect(screen.getByText('text/plain')).toBeInTheDocument();
        expect(screen.getByText(/"abc123"/)).toBeInTheDocument();
      });
    });
  });

  describe('Action Buttons', () => {
    it('has download button in footer', async () => {
      server.use(
        http.post(`${BASE}/api/files/info`, () =>
          HttpResponse.json({
            key: 'test.txt',
            content_length: 1024,
            last_modified: '2024-01-15T10:30:00Z',
            type: 'file',
            content_type: 'text/plain',
          })
        )
      );

      render(<FilePreview {...defaultProps} />);

      await waitFor(() => {
        expect(screen.getByRole('button', { name: /download/i })).toBeInTheDocument();
      });
    });

    it('has close button in footer', async () => {
      server.use(
        http.post(`${BASE}/api/files/info`, () =>
          HttpResponse.json({
            key: 'test.txt',
            content_length: 1024,
            last_modified: '2024-01-15T10:30:00Z',
            type: 'file',
            content_type: 'text/plain',
          })
        )
      );

      render(<FilePreview {...defaultProps} />);

      await waitFor(() => {
        // Check for Close button in footer - using getAllByRole since modal has X button too
        const closeButtons = screen.getAllByRole('button', { name: /close/i });
        expect(closeButtons.length).toBeGreaterThan(0);
      });
    });
  });
});
