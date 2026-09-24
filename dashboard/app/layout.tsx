import type { Metadata, Viewport } from 'next';
import './globals.css';

export const viewport: Viewport = {
  width: 'device-width',
  initialScale: 1,
  maximumScale: 5,
};

export const metadata: Metadata = {
  title: 'UrbanPulse | Multi-City Weather & Air Quality Intelligence Lakehouse',
  description: 'Production-grade data lakehouse monitoring live air quality & weather for Indian megacities with Medallion architecture, dbt quality gates, and next-day ML forecasting.',
  keywords: ['Air Quality', 'Weather', 'Lakehouse', 'Delta Lake', 'DuckDB', 'dbt', 'Machine Learning', 'India AQI'],
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en" className="dark">
      <head>
        <link rel="preconnect" href="https://fonts.googleapis.com" />
        <link rel="preconnect" href="https://fonts.gstatic.com" crossOrigin="anonymous" />
        <link
          href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500;600&display=swap"
          rel="stylesheet"
        />
      </head>
      <body className="h-screen w-screen bg-black overflow-hidden text-[#E2E8F0] antialiased selection:bg-slate-800 selection:text-white">
        {children}
      </body>
    </html>
  );
}
