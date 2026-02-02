/**
 * Main App Component
 * Manages application state and renders ConfigUploader or FileManager
 */
import { useState, useEffect } from 'react';
import { ConfigProvider, Layout, Typography, Alert, Spin, Button } from 'antd';
import { ReloadOutlined } from '@ant-design/icons';
import ConfigUploader from './components/ConfigUploader';
import FileManager from './components/FileManager';
import { healthCheck } from './services/api';

const { Header, Content } = Layout;
const { Title } = Typography;

function App() {
  const [configLoaded, setConfigLoaded] = useState(false);
  const [profiles, setProfiles] = useState([]);
  const [backendStatus, setBackendStatus] = useState(null);
  const [error, setError] = useState(null);

  // Check backend health on mount
  useEffect(() => {
    const checkBackend = async () => {
      try {
        const status = await healthCheck();
        setBackendStatus(status);
      } catch {
        setError('Cannot connect to backend server. Please make sure it is running on http://localhost:8888');
      }
    };

    checkBackend();
  }, []);

  const handleConfigLoaded = (response) => {
    setProfiles(response.profiles);
    setConfigLoaded(true);
    setError(null);
  };

  const handleReset = () => {
    setConfigLoaded(false);
    setProfiles([]);
  };

  return (
    <ConfigProvider
      theme={{
        token: {
          colorPrimary: '#1890ff',
        },
      }}
    >
      <Layout style={{ minHeight: '100vh' }}>
        {/* Show loading screen if backend status is not yet determined */}
        {!backendStatus ? (
          <div style={{
            display: 'flex',
            flexDirection: 'column',
            justifyContent: 'center',
            alignItems: 'center',
            minHeight: '100vh',
            background: '#f0f2f5'
          }}>
            <Spin size="large" />
            <p style={{ marginTop: 16, fontSize: 16, color: '#666' }}>
              Connecting to backend...
            </p>
          </div>
        ) : (
          <>
            {/* Show error banner if there's a backend issue */}
            {error && backendStatus && (
              <Alert
                message="Backend Warning"
                description={error}
                type="warning"
                showIcon
                closable
                onClose={() => setError(null)}
                style={{ borderRadius: 0 }}
              />
            )}

            {/* Main content */}
            {!configLoaded ? (
              <ConfigUploader onConfigLoaded={handleConfigLoaded} />
            ) : (
              <Layout>
                <Header style={{
                  background: '#fff',
                  padding: '0 24px',
                  display: 'flex',
                  alignItems: 'center',
                  justifyContent: 'space-between',
                  boxShadow: '0 2px 8px rgba(0, 0, 0, 0.06)'
                }}>
                  <Title level={3} style={{ margin: 0, color: '#1890ff' }}>
                    MSC Explorer
                  </Title>
                  <Button
                    icon={<ReloadOutlined />}
                    onClick={handleReset}
                  >
                    Change Configuration
                  </Button>
                </Header>
                <Content>
                  <FileManager profiles={profiles} />
                </Content>
              </Layout>
            )}
          </>
        )}
      </Layout>
    </ConfigProvider>
  );
}

export default App;
