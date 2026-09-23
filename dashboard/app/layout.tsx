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
        <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet" />
      </head>
      <body className="min-h-screen bg-[#070B13] text-gray-100 antialiased selection:bg-cyan-500/30 selection:text-cyan-200">
        {children}
      </body>
    </html>
  );
}
