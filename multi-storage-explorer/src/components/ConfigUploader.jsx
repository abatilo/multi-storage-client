/**
 * Configuration Uploader Component
 * Allows users to upload their MSC configuration file using Ant Design components
 */
import { useState } from 'react';
import { Upload, Card, Typography, message, Alert, Space } from 'antd';
import { InboxOutlined } from '@ant-design/icons';
import { uploadConfig } from '../services/api';

const { Dragger } = Upload;
const { Title, Paragraph } = Typography;

const ConfigUploader = ({ onConfigLoaded }) => {
  const [uploading, setUploading] = useState(false);
  const [error, setError] = useState(null);

  const handleUpload = async (file) => {
    // Validate file type
    const validExtensions = ['.json', '.yaml', '.yml'];
    const fileExtension = '.' + file.name.split('.').pop().toLowerCase();
    
    if (!validExtensions.includes(fileExtension)) {
      setError('Please upload a JSON or YAML configuration file');
      message.error('Invalid file type. Please upload a JSON or YAML file.');
      return false;
    }

    setUploading(true);
    setError(null);

    try {
      const response = await uploadConfig(file);
      message.success(`Configuration loaded successfully! Found ${response.profiles.length} profile(s).`);
      onConfigLoaded(response);
    } catch (err) {
      const errorMsg = err.response?.data?.detail || 'Failed to upload configuration';
      setError(errorMsg);
      message.error(errorMsg);
    } finally {
      setUploading(false);
    }

    // Prevent default upload behavior
    return false;
  };

  const uploadProps = {
    name: 'file',
    multiple: false,
    accept: '.json,.yaml,.yml',
    beforeUpload: handleUpload,
    showUploadList: false,
    disabled: uploading,
  };

  return (
    <div style={{ 
      display: 'flex', 
      justifyContent: 'center', 
      alignItems: 'center', 
      minHeight: '100vh',
      background: '#f0f2f5',
      padding: '24px'
    }}>
      <Card 
        style={{ 
          maxWidth: 800, 
          width: '100%',
          boxShadow: '0 4px 6px rgba(0, 0, 0, 0.1)'
        }}
      >
        <Space direction="vertical" size="large" style={{ width: '100%' }}>
          <div style={{ textAlign: 'center' }}>
            <Title level={2}>MSC Explorer</Title>
            <Paragraph type="secondary">
              Upload your MSC configuration file to get started
            </Paragraph>
          </div>

          {error && (
            <Alert
              message="Upload Error"
              description={error}
              type="error"
              closable
              onClose={() => setError(null)}
              showIcon
            />
          )}

          <Dragger {...uploadProps}>
            <p className="ant-upload-drag-icon">
              <InboxOutlined />
            </p>
            <p className="ant-upload-text">
              Click or drag an MSC configuration file to this area to upload
            </p>
            <p className="ant-upload-hint">
              Supports JSON and YAML formats
            </p>
          </Dragger>

          <Card 
            size="small" 
            title="Configuration Format" 
            type="inner"
            style={{ marginTop: 24 }}
          >
            <Paragraph>
              Your configuration file should contain MSC profiles. Example:
            </Paragraph>
            <pre style={{
              background: '#f5f5f5',
              padding: '12px',
              borderRadius: '4px',
              overflow: 'auto',
              fontSize: '12px',
              fontFamily: 'Consolas, Monaco, "Courier New", monospace'
            }}>
{`profiles:
  my-s3:
    storage_provider:
      type: s3
      options:
        base_path: my-bucket
        region_name: us-west-2`}
            </pre>
            <Paragraph type="secondary" style={{ marginTop: 12, fontSize: '12px' }}>
              For more information, see the{' '}
              <a 
                href="https://nvidia.github.io/multi-storage-client/references/configuration.html"
                target="_blank"
                rel="noopener noreferrer"
              >
                MSC Configuration Reference
              </a>
              .
            </Paragraph>
          </Card>
        </Space>
      </Card>
    </div>
  );
};

export default ConfigUploader;


