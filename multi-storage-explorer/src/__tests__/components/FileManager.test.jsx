/**
 * FileManager Component Tests
 * Tests file browsing interface functionality
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { http, HttpResponse } from 'msw';
import { server } from '../../mocks/server';
import FileManager from '../../components/FileManager';

const BASE = 'http://localhost:8888';

// Mock IntersectionObserver for infinite scroll tests
class IntersectionObserverMock {
  constructor(callback) {
    this.callback = callback;
  }
  observe() {}
  unobserve() {}
  disconnect() {}
}

// eslint-disable-next-line no-undef
global.IntersectionObserver = IntersectionObserverMock;

describe('FileManager', () => {
  const defaultProfiles = ['s3-prod', 'gcs-dev', 'local'];

  beforeEach(() => {
    vi.clearAllMocks();
  });

  describe('Rendering', () => {
    it('renders with Profile label', async () => {
      render(<FileManager profiles={defaultProfiles} />);

      await waitFor(() => {
        expect(screen.getByText(/profile/i)).toBeInTheDocument();
      });
    });

    it('renders profile selector', async () => {
      render(<FileManager profiles={defaultProfiles} />);

      await waitFor(() => {
        const select = screen.getByRole('combobox');
        expect(select).toBeInTheDocument();
      });
    });

    it('renders toolbar buttons', async () => {
      render(<FileManager profiles={defaultProfiles} />);

      await waitFor(() => {
        expect(screen.getByRole('button', { name: /refresh/i })).toBeInTheDocument();
      });
    });

    it('renders breadcrumb', async () => {
      render(<FileManager profiles={defaultProfiles} />);

      await waitFor(() => {
        // Breadcrumb contains profile name - using getAllByText since it appears in selector too
        const elements = screen.getAllByText(defaultProfiles[0]);
        expect(elements.length).toBeGreaterThan(0);
      });
    });

    it('renders with empty profiles array', async () => {
      render(<FileManager profiles={[]} />);

      // Should render without crashing
      expect(screen.queryByRole('combobox')).toBeInTheDocument();
    });
  });

  describe('File Loading', () => {
    it('calls listFiles API on mount', async () => {
      let called = false;
      server.use(
        http.post(`${BASE}/api/files/list`, () => {
          called = true;
          return HttpResponse.json({ items: [], count: 0 });
        })
      );

      render(<FileManager profiles={defaultProfiles} />);

      await waitFor(() => {
        expect(called).toBe(true);
      });
    });

    it('displays files from API response', async () => {
      render(<FileManager profiles={defaultProfiles} />);

      await waitFor(() => {
        expect(screen.getByText('readme.txt')).toBeInTheDocument();
        expect(screen.getByText('data.json')).toBeInTheDocument();
      });
    });

    it('displays directories with trailing slash', async () => {
      render(<FileManager profiles={defaultProfiles} />);

      await waitFor(() => {
        expect(screen.getByText('documents/')).toBeInTheDocument();
        expect(screen.getByText('images/')).toBeInTheDocument();
      });
    });

    it('shows error when API fails', async () => {
      server.use(
        http.post(`${BASE}/api/files/list`, () =>
          HttpResponse.json({ detail: 'API error' }, { status: 500 })
        )
      );

      render(<FileManager profiles={defaultProfiles} />);

      await waitFor(() => {
        expect(screen.getByText('API error')).toBeInTheDocument();
      });
    });

    it('shows empty state when no files', async () => {
      server.use(
        http.post(`${BASE}/api/files/list`, () =>
          HttpResponse.json({ items: [], count: 0 })
        )
      );

      render(<FileManager profiles={defaultProfiles} />);

      await waitFor(() => {
        expect(screen.getByText(/this directory is empty/i)).toBeInTheDocument();
      });
    });
  });

  describe('Refresh', () => {
    it('reloads files on refresh button click', async () => {
      let callCount = 0;
      server.use(
        http.post(`${BASE}/api/files/list`, () => {
          callCount++;
          return HttpResponse.json({
            items: [
              {
                name: 'file1.txt',
                key: 'file1.txt',
                type: 'file',
                size: 1024,
                last_modified: '2024-01-15T10:00:00Z',
                is_directory: false,
              },
            ],
            count: 1,
          });
        })
      );

      render(<FileManager profiles={defaultProfiles} />);

      await waitFor(() => {
        expect(screen.getByText('file1.txt')).toBeInTheDocument();
      });

      const initialCallCount = callCount;
      const refreshButton = screen.getByRole('button', { name: /refresh/i });
      await userEvent.click(refreshButton);

      await waitFor(() => {
        expect(callCount).toBeGreaterThan(initialCallCount);
      });
    });
  });

  describe('View Mode', () => {
    it('defaults to list view with table', async () => {
      render(<FileManager profiles={defaultProfiles} />);

      await waitFor(() => {
        expect(screen.getByRole('table')).toBeInTheDocument();
      });
    });
  });

  describe('Directory Navigation', () => {
    it('navigates into directory on click', async () => {
      server.use(
        http.post(`${BASE}/api/files/list`, ({ request }) => {
          return request.json().then((body) => {
            if (body.url.includes('/subfolder')) {
              return HttpResponse.json({
                items: [
                  {
                    name: 'nested.txt',
                    key: 'subfolder/nested.txt',
                    type: 'file',
                    size: 512,
                    last_modified: '2024-01-15T10:00:00Z',
                    is_directory: false,
                  },
                ],
                count: 1,
              });
            }
            return HttpResponse.json({
              items: [
                {
                  name: 'subfolder',
                  key: 'subfolder/',
                  type: 'directory',
                  size: 0,
                  last_modified: '2024-01-15T10:00:00Z',
                  is_directory: true,
                },
              ],
              count: 1,
            });
          });
        })
      );

      render(<FileManager profiles={defaultProfiles} />);

      await waitFor(() => {
        expect(screen.getByText('subfolder/')).toBeInTheDocument();
      });

      const folderLink = screen.getByText('subfolder/');
      await userEvent.click(folderLink);

      await waitFor(() => {
        expect(screen.getByText('nested.txt')).toBeInTheDocument();
      });
    });
  });

  describe('File Size Formatting', () => {
    it('formats file sizes correctly in table', async () => {
      server.use(
        http.post(`${BASE}/api/files/list`, () =>
          HttpResponse.json({
            items: [
              {
                name: 'tiny.txt',
                key: 'tiny.txt',
                type: 'file',
                size: 512,
                last_modified: '2024-01-15T10:00:00Z',
                is_directory: false,
              },
              {
                name: 'large.bin',
                key: 'large.bin',
                type: 'file',
                size: 1048576,
                last_modified: '2024-01-15T10:00:00Z',
                is_directory: false,
              },
            ],
            count: 2,
          })
        )
      );

      render(<FileManager profiles={defaultProfiles} />);

      await waitFor(() => {
        expect(screen.getByText(/512 B/i)).toBeInTheDocument();
        expect(screen.getByText(/1 MB/i)).toBeInTheDocument();
      });
    });

    it('formats zero byte files', async () => {
      server.use(
        http.post(`${BASE}/api/files/list`, () =>
          HttpResponse.json({
            items: [
              {
                name: 'empty.txt',
                key: 'empty.txt',
                type: 'file',
                size: 0,
                last_modified: '2024-01-15T10:00:00Z',
                is_directory: false,
              },
            ],
            count: 1,
          })
        )
      );

      render(<FileManager profiles={defaultProfiles} />);

      await waitFor(() => {
        expect(screen.getByText('0 B')).toBeInTheDocument();
      });
    });
  });

  describe('Table Features', () => {
    it('shows file type tags', async () => {
      render(<FileManager profiles={defaultProfiles} />);

      await waitFor(() => {
        expect(screen.getByText(/file/i)).toBeInTheDocument();
      });
    });

    it('shows item count', async () => {
      render(<FileManager profiles={defaultProfiles} />);

      await waitFor(() => {
        expect(screen.getByText(/\d+ items?/i)).toBeInTheDocument();
      });
    });
  });

  describe('Breadcrumb Navigation', () => {
    it('updates breadcrumb after directory navigation', async () => {
      server.use(
        http.post(`${BASE}/api/files/list`, ({ request }) => {
          return request.json().then((body) => {
            if (body.url.includes('/folder1')) {
              return HttpResponse.json({
                items: [],
                count: 0,
              });
            }
            return HttpResponse.json({
              items: [
                {
                  name: 'folder1',
                  key: 'folder1/',
                  type: 'directory',
                  size: 0,
                  last_modified: '2024-01-15T10:00:00Z',
                  is_directory: true,
                },
              ],
              count: 1,
            });
          });
        })
      );

      render(<FileManager profiles={defaultProfiles} />);

      await waitFor(() => {
        expect(screen.getByText('folder1/')).toBeInTheDocument();
      });

      const folderLink = screen.getByText('folder1/');
      await userEvent.click(folderLink);

      await waitFor(() => {
        expect(screen.getByText('folder1')).toBeInTheDocument();
      });
    });
  });

  describe('Profile Selection', () => {
    it('shows profile selector with current profile', async () => {
      render(<FileManager profiles={defaultProfiles} />);

      await waitFor(() => {
        const select = screen.getByRole('combobox');
        expect(select).toBeInTheDocument();
        // Profile should be displayed in the breadcrumb or selector
        const profileElements = screen.getAllByText(defaultProfiles[0]);
        expect(profileElements.length).toBeGreaterThan(0);
      });
    });
  });

  describe('Error Handling', () => {
    it('displays error alert when API fails', async () => {
      server.use(
        http.post(`${BASE}/api/files/list`, () =>
          HttpResponse.json({ detail: 'Connection error' }, { status: 500 })
        )
      );

      render(<FileManager profiles={defaultProfiles} />);

      await waitFor(() => {
        expect(screen.getByText(/connection error/i)).toBeInTheDocument();
      });
    });
  });

  describe('Date Formatting', () => {
    it('displays formatted date in table', async () => {
      server.use(
        http.post(`${BASE}/api/files/list`, () =>
          HttpResponse.json({
            items: [
              {
                name: 'test.txt',
                key: 'test.txt',
                type: 'file',
                size: 1024,
                last_modified: '2024-01-16T16:42:56Z',
                is_directory: false,
              },
            ],
            count: 1,
          })
        )
      );

      render(<FileManager profiles={defaultProfiles} />);

      await waitFor(() => {
        // Check that the date is formatted (contains month name and time)
        expect(screen.getByText(/January.*2024/)).toBeInTheDocument();
      });
    });
  });

  describe('Alert Dismissal', () => {
    it('closes error alert when close button is clicked', async () => {
      server.use(
        http.post(`${BASE}/api/files/list`, () =>
          HttpResponse.json({ detail: 'Test error' }, { status: 500 })
        )
      );

      render(<FileManager profiles={defaultProfiles} />);

      // Wait for error alert to appear
      let alert;
      await waitFor(() => {
        alert = screen.getByRole('alert');
        expect(alert).toBeInTheDocument();
        expect(within(alert).getByText('Test error')).toBeInTheDocument();
      });

      // Find the close button within the alert
      const closeButton = within(alert).getByRole('button', { name: /close/i });
      await userEvent.click(closeButton);

      // Verify alert is removed
      await waitFor(() => {
        expect(screen.queryByRole('alert')).not.toBeInTheDocument();
      });
    });
  });
});
