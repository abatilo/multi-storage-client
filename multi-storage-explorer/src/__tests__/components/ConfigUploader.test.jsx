/**
 * ConfigUploader Component Tests
 * Tests configuration file upload component rendering and upload functionality
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, waitFor, fireEvent } from '@testing-library/react';
import ConfigUploader from '../../components/ConfigUploader';

// Mock the uploadConfig API function directly
vi.mock('../../services/api', () => ({
  uploadConfig: vi.fn(),
}));

import { uploadConfig } from '../../services/api';

describe('ConfigUploader', () => {
  const mockOnConfigLoaded = vi.fn();

  beforeEach(() => {
    mockOnConfigLoaded.mockClear();
    uploadConfig.mockClear();
  });

  describe('Rendering', () => {
    it('renders upload area with title', () => {
      render(<ConfigUploader onConfigLoaded={mockOnConfigLoaded} />);
      expect(screen.getByText(/msc explorer/i)).toBeInTheDocument();
    });

    it('shows supported file formats', () => {
      render(<ConfigUploader onConfigLoaded={mockOnConfigLoaded} />);
      expect(screen.getByText(/supports json and yaml formats/i)).toBeInTheDocument();
    });

    it('renders file input element', () => {
      render(<ConfigUploader onConfigLoaded={mockOnConfigLoaded} />);
      const input = document.querySelector('input[type="file"]');
      expect(input).toBeInTheDocument();
    });

    it('file input accepts correct file types', () => {
      render(<ConfigUploader onConfigLoaded={mockOnConfigLoaded} />);
      const input = document.querySelector('input[type="file"]');
      expect(input.accept).toContain('.json');
      expect(input.accept).toContain('.yaml');
      expect(input.accept).toContain('.yml');
    });

    it('shows inbox icon in upload area', () => {
      render(<ConfigUploader onConfigLoaded={mockOnConfigLoaded} />);
      expect(screen.getByRole('img', { name: /inbox/i })).toBeInTheDocument();
    });
  });

  describe('File Upload - Validation', () => {
    it('rejects .txt files and shows error', async () => {
      render(<ConfigUploader onConfigLoaded={mockOnConfigLoaded} />);
      
      const input = document.querySelector('input[type="file"]');
      const invalidFile = new File(['content'], 'test.txt', { type: 'text/plain' });
      
      // Create a change event with the file
      Object.defineProperty(input, 'files', { value: [invalidFile] });
      fireEvent.change(input);
      
      await waitFor(() => {
        expect(screen.getByText(/please upload a json or yaml configuration file/i)).toBeInTheDocument();
      });
      expect(mockOnConfigLoaded).not.toHaveBeenCalled();
      expect(uploadConfig).not.toHaveBeenCalled();
    });

    it('rejects .exe files and shows error', async () => {
      render(<ConfigUploader onConfigLoaded={mockOnConfigLoaded} />);
      
      const input = document.querySelector('input[type="file"]');
      const exeFile = new File(['binary'], 'app.exe', { type: 'application/octet-stream' });
      
      Object.defineProperty(input, 'files', { value: [exeFile] });
      fireEvent.change(input);
      
      await waitFor(() => {
        expect(screen.getByText(/please upload a json or yaml configuration file/i)).toBeInTheDocument();
      });
      expect(mockOnConfigLoaded).not.toHaveBeenCalled();
    });

    it('rejects .csv files', async () => {
      render(<ConfigUploader onConfigLoaded={mockOnConfigLoaded} />);
      
      const input = document.querySelector('input[type="file"]');
      const csvFile = new File(['a,b,c'], 'data.csv', { type: 'text/csv' });
      
      Object.defineProperty(input, 'files', { value: [csvFile] });
      fireEvent.change(input);
      
      await waitFor(() => {
        expect(screen.getByText(/please upload a json or yaml configuration file/i)).toBeInTheDocument();
      });
    });
  });

  describe('File Upload - Success', () => {
    it('accepts .json file and calls API', async () => {
      uploadConfig.mockResolvedValueOnce({ profiles: ['profile1', 'profile2'] });
      
      render(<ConfigUploader onConfigLoaded={mockOnConfigLoaded} />);
      
      const input = document.querySelector('input[type="file"]');
      const jsonFile = new File(['{"profiles":{}}'], 'config.json', { type: 'application/json' });
      
      Object.defineProperty(input, 'files', { value: [jsonFile] });
      fireEvent.change(input);
      
      await waitFor(() => {
        expect(uploadConfig).toHaveBeenCalledWith(jsonFile);
      });
      
      await waitFor(() => {
        expect(mockOnConfigLoaded).toHaveBeenCalledWith({ profiles: ['profile1', 'profile2'] });
      });
    });

    it('accepts .yaml file and calls API', async () => {
      uploadConfig.mockResolvedValueOnce({ profiles: ['yaml-profile'] });
      
      render(<ConfigUploader onConfigLoaded={mockOnConfigLoaded} />);
      
      const input = document.querySelector('input[type="file"]');
      const yamlFile = new File(['profiles: {}'], 'config.yaml', { type: 'application/x-yaml' });
      
      Object.defineProperty(input, 'files', { value: [yamlFile] });
      fireEvent.change(input);
      
      await waitFor(() => {
        expect(uploadConfig).toHaveBeenCalledWith(yamlFile);
      });
      
      await waitFor(() => {
        expect(mockOnConfigLoaded).toHaveBeenCalled();
      });
    });

    it('accepts .yml file and calls API', async () => {
      uploadConfig.mockResolvedValueOnce({ profiles: ['yml-profile'] });
      
      render(<ConfigUploader onConfigLoaded={mockOnConfigLoaded} />);
      
      const input = document.querySelector('input[type="file"]');
      const ymlFile = new File(['profiles: {}'], 'config.yml', { type: 'application/x-yaml' });
      
      Object.defineProperty(input, 'files', { value: [ymlFile] });
      fireEvent.change(input);
      
      await waitFor(() => {
        expect(uploadConfig).toHaveBeenCalledWith(ymlFile);
      });
      
      await waitFor(() => {
        expect(mockOnConfigLoaded).toHaveBeenCalled();
      });
    });
  });

  describe('File Upload - Error Handling', () => {
    it('shows error when API returns error with detail', async () => {
      uploadConfig.mockRejectedValueOnce({
        response: { data: { detail: 'Invalid configuration format' } }
      });
      
      render(<ConfigUploader onConfigLoaded={mockOnConfigLoaded} />);
      
      const input = document.querySelector('input[type="file"]');
      const jsonFile = new File(['{}'], 'config.json', { type: 'application/json' });
      
      Object.defineProperty(input, 'files', { value: [jsonFile] });
      fireEvent.change(input);
      
      await waitFor(() => {
        expect(screen.getByText(/invalid configuration format/i)).toBeInTheDocument();
      });
      expect(mockOnConfigLoaded).not.toHaveBeenCalled();
    });

    it('shows generic error when API returns error without detail', async () => {
      uploadConfig.mockRejectedValueOnce(new Error('Network error'));
      
      render(<ConfigUploader onConfigLoaded={mockOnConfigLoaded} />);
      
      const input = document.querySelector('input[type="file"]');
      const jsonFile = new File(['{}'], 'config.json', { type: 'application/json' });
      
      Object.defineProperty(input, 'files', { value: [jsonFile] });
      fireEvent.change(input);
      
      await waitFor(() => {
        expect(screen.getByText(/failed to upload configuration/i)).toBeInTheDocument();
      });
      expect(mockOnConfigLoaded).not.toHaveBeenCalled();
    });

    it('shows server error message', async () => {
      uploadConfig.mockRejectedValueOnce({
        response: { data: { detail: 'Server error occurred' } }
      });
      
      render(<ConfigUploader onConfigLoaded={mockOnConfigLoaded} />);
      
      const input = document.querySelector('input[type="file"]');
      const jsonFile = new File(['{}'], 'config.json', { type: 'application/json' });
      
      Object.defineProperty(input, 'files', { value: [jsonFile] });
      fireEvent.change(input);
      
      await waitFor(() => {
        expect(screen.getByText(/server error occurred/i)).toBeInTheDocument();
      });
    });
  });

  describe('Error Alert', () => {
    it('shows error alert with Upload Error title', async () => {
      render(<ConfigUploader onConfigLoaded={mockOnConfigLoaded} />);
      
      const input = document.querySelector('input[type="file"]');
      const invalidFile = new File(['content'], 'test.txt', { type: 'text/plain' });
      
      Object.defineProperty(input, 'files', { value: [invalidFile] });
      fireEvent.change(input);
      
      await waitFor(() => {
        expect(screen.getByText('Upload Error')).toBeInTheDocument();
      });
    });

    it('error alert is closable', async () => {
      render(<ConfigUploader onConfigLoaded={mockOnConfigLoaded} />);
      
      const input = document.querySelector('input[type="file"]');
      const invalidFile = new File(['content'], 'test.txt', { type: 'text/plain' });
      
      Object.defineProperty(input, 'files', { value: [invalidFile] });
      fireEvent.change(input);
      
      await waitFor(() => {
        expect(screen.getByText('Upload Error')).toBeInTheDocument();
      });

      // Find and click close button
      const closeButton = screen.getByRole('button', { name: /close/i });
      fireEvent.click(closeButton);

      await waitFor(() => {
        expect(screen.queryByText('Upload Error')).not.toBeInTheDocument();
      });
    });
  });
});
