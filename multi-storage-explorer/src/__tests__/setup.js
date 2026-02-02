/**
 * Vitest Test Setup
 * Global configuration for all tests
 */
import { beforeAll, afterEach, afterAll, vi } from 'vitest';
import { cleanup } from '@testing-library/react';
import '@testing-library/jest-dom/vitest';
import { server } from '../mocks/server';

// Start MSW server before all tests
beforeAll(() => {
  server.listen({ onUnhandledRequest: 'error' });
});

// Reset handlers after each test (important for test isolation)
afterEach(() => {
  cleanup();
  server.resetHandlers();
});

// Close MSW server after all tests
afterAll(() => {
  server.close();
});

// Mock window.matchMedia for Ant Design components
Object.defineProperty(window, 'matchMedia', {
  writable: true,
  value: vi.fn().mockImplementation((query) => ({
    matches: false,
    media: query,
    onchange: null,
    addListener: vi.fn(),
    removeListener: vi.fn(),
    addEventListener: vi.fn(),
    removeEventListener: vi.fn(),
    dispatchEvent: vi.fn(),
  })),
});

// Mock window.scrollTo
window.scrollTo = vi.fn();

// Mock ResizeObserver for Ant Design
class ResizeObserverMock {
  observe() {}
  unobserve() {}
  disconnect() {}
}
window.ResizeObserver = ResizeObserverMock;

// Mock getComputedStyle for Ant Design
const originalGetComputedStyle = window.getComputedStyle;
window.getComputedStyle = (element) => {
  return originalGetComputedStyle(element);
};

// Mock URL.createObjectURL for file downloads
window.URL.createObjectURL = vi.fn(() => 'blob:mock-url');
window.URL.revokeObjectURL = vi.fn();

