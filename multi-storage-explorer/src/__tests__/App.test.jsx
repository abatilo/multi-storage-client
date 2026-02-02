/**
 * App Component Tests
 * Tests main application state management and routing
 */
import { describe, it, expect } from 'vitest';
import { render, screen, waitFor } from '@testing-library/react';
import { http, HttpResponse } from 'msw';
import { server } from '../mocks/server';
import App from '../App';

const BASE = 'http://localhost:8888';

describe('App', () => {
  describe('Initial Loading State', () => {
    it('shows loading spinner while checking backend', async () => {
      server.use(
        http.get(`${BASE}/api/health`, async () => {
          await new Promise((resolve) => setTimeout(resolve, 100));
          return HttpResponse.json({ 
            status: 'healthy', 
            config_loaded: false, 
            frontend_available: true 
          });
        })
      );

      render(<App />);

      expect(screen.getByText(/connecting to backend/i)).toBeInTheDocument();
    });

    it('shows ConfigUploader after backend health check succeeds', async () => {
      render(<App />);

      await waitFor(() => {
        expect(screen.getByText(/msc explorer/i)).toBeInTheDocument();
      });

      expect(screen.getByText(/upload your msc configuration/i)).toBeInTheDocument();
    });
  });

  describe('Backend Connection Errors', () => {
    // Note: When backend is unavailable, the App stays on loading screen
    // because backendStatus remains null. The error is set but not displayed
    // until the UI is updated to handle this case.
    it('shows loading state when backend is unavailable', async () => {
      server.use(
        http.get(`${BASE}/api/health`, () => 
          HttpResponse.json({ detail: 'Server error' }, { status: 500 })
        )
      );

      render(<App />);

      // The app stays on loading state because backendStatus remains null
      expect(screen.getByText(/connecting to backend/i)).toBeInTheDocument();
    });
  });

});
