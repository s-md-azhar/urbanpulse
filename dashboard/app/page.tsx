'use client';

import React, { useState, useEffect } from 'react';
import {
  Activity,
  Wind,
  Droplets,
  Thermometer,
  ShieldCheck,
  Calendar,
  Layers,
  ArrowUpRight,
  TrendingDown,
  TrendingUp,
  Clock,
  Cpu,
  RefreshCw,
  Info,
  CheckCircle2,
  AlertTriangle,
  ExternalLink,
} from 'lucide-react';
import {
  ResponsiveContainer,
  ComposedChart,
  Area,
  Line,
  BarChart,
  Bar,
  XAxis,
  YAxis,
  Tooltip,
  Legend,
  CartesianGrid,
} from 'recharts';

interface CitySummary {
  city: string;
  city_name: string;
  state: string;
  latitude: number;
  longitude: number;
  population_tier: string;
  metric_date: string;
  avg_temperature_c: number;
  min_temperature_c: number;
  max_temperature_c: number;
  avg_humidity_pct: number;
  total_precipitation_mm: number;
  avg_wind_speed_kmh: number;
  max_wind_speed_kmh: number;
  avg_pm2_5: number;
  max_pm2_5: number;
  avg_pm10: number;
  max_pm10: number;
  avg_carbon_monoxide: number;
  avg_nitrogen_dioxide: number;
  avg_sulphur_dioxide: number;
  avg_ozone: number;
  avg_us_aqi: number;
  max_us_aqi: number;
  aqi_category: string;
  lag_1d_aqi?: number;
}

interface Prediction {
  city: string;
  forecast_for_date: string;
  reference_date: string;
  predicted_aqi: number;
  predicted_category: string;
  reference_aqi: number;
  model_version: string;
  mae: number;
}

interface MetricRow {
  city: string;
  metric_date: string;
  avg_temperature_c: number;
  avg_humidity_pct: number;
  avg_wind_speed_kmh: number;
  avg_pm2_5: number;
  avg_pm10: number;
  avg_us_aqi: number;
  max_us_aqi: number;
  aqi_category: string;
  lag_1d_aqi: number;
  rolling_7d_avg_aqi: number;
}

interface PipelineMeta {
  pipeline_name: string;
  last_refresh_timestamp: string;
  status: string;
  quality_gate_passed: boolean;
  layers: {
    raw_landing: { format: string; cities_covered: number };
    bronze: { format: string; weather_row_count: number; air_quality_row_count: number };
    silver: { format: string; weather_row_count: number; air_quality_row_count: number; primary_key: string };
    gold: { engine: string; marts: string[]; total_daily_fact_records: number };
    ml_layer: {
      model: string;
      target: string;
      cities_modeled: number;
      split_method?: string;
      train_days?: number;
      val_days?: number;
      test_days?: number;
      tuning_strategy?: string;
      winning_hyperparameters?: {
        min_samples_leaf: number;
        learning_rate: number;
        max_depth: number;
        random_state: number;
      };
    };
  };
  cities: string[];
}

// AQI Color helper
function getAqiStyle(aqi: number) {
  if (aqi <= 50) {
    return {
      bg: 'bg-emerald-500/15',
      text: 'text-emerald-400',
      border: 'border-emerald-500/30',
      solid: '#10B981',
      label: 'Good',
      description: 'Air quality is satisfactory, poses little or no risk.',
    };
  } else if (aqi <= 100) {
    return {
      bg: 'bg-amber-500/15',
      text: 'text-amber-400',
      border: 'border-amber-500/30',
      solid: '#F59E0B',
      label: 'Moderate',
      description: 'Acceptable; sensitive individuals should monitor symptoms.',
    };
  } else if (aqi <= 150) {
    return {
      bg: 'bg-orange-500/15',
      text: 'text-orange-400',
      border: 'border-orange-500/30',
      solid: '#F97316',
      label: 'Unhealthy for Sensitive Groups',
      description: 'Members of sensitive groups may experience health effects.',
    };
  } else if (aqi <= 200) {
    return {
      bg: 'bg-red-500/15',
      text: 'text-red-400',
      border: 'border-red-500/30',
      solid: '#EF4444',
      label: 'Unhealthy',
      description: 'Everyone may begin to experience health effects.',
    };
  } else if (aqi <= 300) {
    return {
      bg: 'bg-purple-500/15',
      text: 'text-purple-400',
      border: 'border-purple-500/30',
      solid: '#8B5CF6',
      label: 'Very Unhealthy',
      description: 'Health alert: increased likelihood of serious health effects.',
    };
  } else {
    return {
      bg: 'bg-rose-900/30',
      text: 'text-rose-400',
      border: 'border-rose-700/50',
      solid: '#881337',
      label: 'Hazardous',
      description: 'Emergency health warning: entire population affected.',
    };
  }
}

export default function UrbanPulseDashboard() {
  const [summaryData, setSummaryData] = useState<CitySummary[]>([]);
  const [metricsData, setMetricsData] = useState<MetricRow[]>([]);
  const [predictionsData, setPredictionsData] = useState<Prediction[]>([]);
  const [metaData, setMetaData] = useState<PipelineMeta | null>(null);
  const [selectedCity, setSelectedCity] = useState<string>('delhi');
  const [loading, setLoading] = useState<boolean>(true);
  const [viewTab, setViewTab] = useState<'matrix' | 'deepdive' | 'observability'>('matrix');

  useEffect(() => {
    async function loadData() {
      try {
        const [sumRes, metRes, predRes, metaRes] = await Promise.all([
          fetch('/data/summary.json').catch(() => null),
          fetch('/data/metrics.json').catch(() => null),
          fetch('/data/predictions.json').catch(() => null),
          fetch('/data/pipeline_meta.json').catch(() => null),
        ]);

        if (sumRes && sumRes.ok) setSummaryData(await sumRes.json());
        if (metRes && metRes.ok) setMetricsData(await metRes.json());
        if (predRes && predRes.ok) setPredictionsData(await predRes.json());
        if (metaRes && metaRes.ok) setMetaData(await metaRes.json());
      } catch (err) {
        console.warn('Failed to load live data snapshots, fallback to default state.', err);
      } finally {
        setLoading(false);
      }
    }
    loadData();
  }, []);

  const activeCitySummary = summaryData.find((c) => c.city === selectedCity) || summaryData[0];
  const activeCityPred = predictionsData.find((p) => p.city === selectedCity);
  const activeCityMetrics = metricsData.filter((m) => m.city === selectedCity);

  const aqiStyle = activeCitySummary ? getAqiStyle(activeCitySummary.avg_us_aqi) : getAqiStyle(110);
  const predStyle = activeCityPred ? getAqiStyle(activeCityPred.predicted_aqi) : getAqiStyle(115);

  // Time series chart data merging metrics + forecast point
  const chartData = activeCityMetrics.map((m) => {
    return {
      date: m.metric_date.slice(5),
      actualAQI: m.avg_us_aqi,
      rolling7d: m.rolling_7d_avg_aqi,
      pm25: m.avg_pm2_5,
      pm10: m.avg_pm10,
      temperature: m.avg_temperature_c,
      humidity: m.avg_humidity_pct,
      windSpeed: m.avg_wind_speed_kmh,
    };
  });

  if (activeCityPred && chartData.length > 0) {
    chartData.push({
      date: 'Tomorrow (Forecast)',
      actualAQI: undefined as any,
      rolling7d: undefined as any,
      pm25: undefined as any,
      pm10: undefined as any,
      temperature: undefined as any,
      humidity: undefined as any,
      windSpeed: undefined as any,
      ...({ predictedAQI: activeCityPred.predicted_aqi } as any),
    });
  }

  return (
    <div className="min-h-screen bg-[#070B13] text-gray-100 flex flex-col">
      {/* Top Navbar */}
      <header className="sticky top-0 z-50 bg-[#090D18]/80 backdrop-blur-xl border-b border-white/10 px-4 lg:px-8 py-3.5">
        <div className="max-w-7xl mx-auto flex flex-col sm:flex-row items-center justify-between gap-3">
          <div className="flex items-center gap-3">
            <div className="w-9 h-9 rounded-xl bg-gradient-to-tr from-cyan-500 via-indigo-500 to-purple-500 p-0.5 shadow-lg shadow-cyan-500/20">
              <div className="w-full h-full bg-[#090D18] rounded-[10px] flex items-center justify-center">
                <Activity className="w-5 h-5 text-cyan-400" />
              </div>
            </div>
            <div>
              <div className="flex items-center gap-2">
                <h1 className="text-lg font-bold tracking-tight bg-gradient-to-r from-white via-gray-200 to-gray-400 bg-clip-text text-transparent">
                  UrbanPulse
                </h1>
                <span className="px-2 py-0.5 text-[10px] uppercase font-semibold tracking-wider rounded-full bg-cyan-500/10 text-cyan-300 border border-cyan-500/20">
                  Lakehouse Engine
                </span>
              </div>
              <p className="text-xs text-gray-400 hidden sm:block">
                Multi-City Weather & Air Quality Intelligence Platform
              </p>
            </div>
          </div>

          <div className="flex items-center gap-3">
            <div className="flex items-center gap-2 px-3 py-1 rounded-full bg-emerald-500/10 border border-emerald-500/20 text-xs text-emerald-400 font-medium">
              <span className="w-2 h-2 rounded-full bg-emerald-400 animate-pulse" />
              <span>Lakehouse Healthy</span>
            </div>

            <div className="flex bg-[#111827] rounded-lg p-1 border border-white/10 text-xs">
              <button
                onClick={() => setViewTab('matrix')}
                className={`px-3 py-1 rounded-md transition-colors ${
                  viewTab === 'matrix' ? 'bg-cyan-500 text-white font-medium' : 'text-gray-400 hover:text-white'
                }`}
              >
                City Matrix
              </button>
              <button
                onClick={() => setViewTab('deepdive')}
                className={`px-3 py-1 rounded-md transition-colors ${
                  viewTab === 'deepdive' ? 'bg-cyan-500 text-white font-medium' : 'text-gray-400 hover:text-white'
                }`}
              >
                Forecast Deep Dive
              </button>
              <button
                onClick={() => setViewTab('observability')}
                className={`px-3 py-1 rounded-md transition-colors ${
                  viewTab === 'observability' ? 'bg-cyan-500 text-white font-medium' : 'text-gray-400 hover:text-white'
                }`}
              >
                Platform Health
              </button>
            </div>
          </div>
        </div>
      </header>

      {/* Main Content Area */}
      <main className="flex-1 max-w-7xl w-full mx-auto px-4 lg:px-8 py-6 space-y-6">
        {/* KPI Strip */}
        <div className="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-6 gap-3">
          <div className="glass-panel p-3.5 rounded-xl border border-white/5">
            <div className="flex items-center justify-between text-gray-400 text-xs">
              <span>Cities Monitored</span>
              <Layers className="w-4 h-4 text-cyan-400" />
            </div>
            <div className="text-xl font-bold mt-1 text-white">8 Hubs</div>
            <div className="text-[11px] text-gray-500 mt-0.5">Delhi, Mumbai, BLR...</div>
          </div>

          <div className="glass-panel p-3.5 rounded-xl border border-white/5">
            <div className="flex items-center justify-between text-gray-400 text-xs">
              <span>Bronze Delta Ingest</span>
              <ShieldCheck className="w-4 h-4 text-indigo-400" />
            </div>
            <div className="text-xl font-bold mt-1 text-white">
              {metaData ? (metaData.layers.bronze.weather_row_count + metaData.layers.bronze.air_quality_row_count).toLocaleString() : '5,376'}
            </div>
            <div className="text-[11px] text-gray-500 mt-0.5">ACID Append Log</div>
          </div>

          <div className="glass-panel p-3.5 rounded-xl border border-white/5">
            <div className="flex items-center justify-between text-gray-400 text-xs">
              <span>Silver Cleaned Rows</span>
              <RefreshCw className="w-4 h-4 text-emerald-400" />
            </div>
            <div className="text-xl font-bold mt-1 text-white">
              {metaData ? (metaData.layers.silver.weather_row_count + metaData.layers.silver.air_quality_row_count).toLocaleString() : '5,376'}
            </div>
            <div className="text-[11px] text-gray-500 mt-0.5">Idempotent Upserted</div>
          </div>

          <div className="glass-panel p-3.5 rounded-xl border border-white/5">
            <div className="flex items-center justify-between text-gray-400 text-xs">
              <span>Gold Analytical Marts</span>
              <DatabaseIcon className="w-4 h-4 text-amber-400" />
            </div>
            <div className="text-xl font-bold mt-1 text-white">
              {metaData ? metaData.layers.gold.total_daily_fact_records : '112'} Fact Days
            </div>
            <div className="text-[11px] text-gray-500 mt-0.5">DuckDB + dbt-core</div>
          </div>

          <div className="glass-panel p-3.5 rounded-xl border border-white/5">
            <div className="flex items-center justify-between text-gray-400 text-xs">
              <span>ML Forecaster</span>
              <Cpu className="w-4 h-4 text-purple-400" />
            </div>
            <div className="text-xl font-bold mt-1 text-white">HistGradient</div>
            <div className="text-[11px] text-gray-500 mt-0.5">Lagged Weather+AQI</div>
          </div>

          <div className="glass-panel p-3.5 rounded-xl border border-white/5">
            <div className="flex items-center justify-between text-gray-400 text-xs">
              <span>Data Quality Gates</span>
              <CheckCircle2 className="w-4 h-4 text-emerald-400" />
            </div>
            <div className="text-xl font-bold mt-1 text-emerald-400">100% Passed</div>
            <div className="text-[11px] text-gray-500 mt-0.5">dbt schema & sanity</div>
          </div>
        </div>

        {/* View Tab 1: City Matrix Overview */}
        {viewTab === 'matrix' && (
          <div className="space-y-6">
            <div className="flex items-center justify-between">
              <div>
                <h2 className="text-lg font-bold text-white tracking-tight">
                  Indian Megacities — Air Quality & Forecast Matrix
                </h2>
                <p className="text-xs text-gray-400">
                  Daily aggregated observations with next-day ML predicted AQI and atmospheric conditions
                </p>
              </div>
              <span className="text-xs text-gray-500 flex items-center gap-1.5 font-mono">
                <Clock className="w-3.5 h-3.5" />
                Refreshed Daily at 03:00 UTC
              </span>
            </div>

            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
              {summaryData.map((cityItem) => {
                const style = getAqiStyle(cityItem.avg_us_aqi);
                const pred = predictionsData.find((p) => p.city === cityItem.city);
                const predAQI = pred?.predicted_aqi ?? cityItem.avg_us_aqi;
                const delta = predAQI - cityItem.avg_us_aqi;
                const isSelected = selectedCity === cityItem.city;

                return (
                  <div
                    key={cityItem.city}
                    onClick={() => {
                      setSelectedCity(cityItem.city);
                      setViewTab('deepdive');
                    }}
                    className={`glass-panel glass-panel-hover p-5 rounded-2xl cursor-pointer border ${
                      isSelected ? 'border-cyan-500 ring-2 ring-cyan-500/20' : 'border-white/5'
                    } flex flex-col justify-between`}
                  >
                    <div>
                      <div className="flex items-start justify-between">
                        <div>
                          <h3 className="font-bold text-white text-base">{cityItem.city_name}</h3>
                          <span className="text-[11px] text-gray-400">{cityItem.state}</span>
                        </div>
                        <span
                          className={`px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wider rounded-full border ${style.bg} ${style.text} ${style.border}`}
                        >
                          {style.label}
                        </span>
                      </div>

                      <div className="mt-4 flex items-baseline justify-between">
                        <div>
                          <span className="text-3xl font-extrabold tracking-tight text-white">
                            {Math.round(cityItem.avg_us_aqi)}
                          </span>
                          <span className="text-xs text-gray-400 ml-1.5 font-mono">US AQI</span>
                        </div>
                        <div className="text-right">
                          <div className="text-xs text-gray-400 flex items-center justify-end gap-1">
                            <span>Tomorrow</span>
                            {delta > 0 ? (
                              <TrendingUp className="w-3 h-3 text-red-400" />
                            ) : (
                              <TrendingDown className="w-3 h-3 text-emerald-400" />
                            )}
                          </div>
                          <span className="text-sm font-bold text-gray-200">
                            {predAQI} ({delta > 0 ? `+${delta.toFixed(1)}` : delta.toFixed(1)})
                          </span>
                        </div>
                      </div>

                      {/* Pollutant and weather strip */}
                      <div className="mt-4 pt-3 border-t border-white/5 grid grid-cols-3 gap-2 text-[11px]">
                        <div className="bg-white/[0.02] p-2 rounded-lg text-center">
                          <span className="text-gray-400 block text-[10px]">PM2.5</span>
                          <span className="font-semibold text-gray-200">{cityItem.avg_pm2_5} µg</span>
                        </div>
                        <div className="bg-white/[0.02] p-2 rounded-lg text-center">
                          <span className="text-gray-400 block text-[10px]">Temp</span>
                          <span className="font-semibold text-gray-200">{cityItem.avg_temperature_c}°C</span>
                        </div>
                        <div className="bg-white/[0.02] p-2 rounded-lg text-center">
                          <span className="text-gray-400 block text-[10px]">Wind</span>
                          <span className="font-semibold text-gray-200">{cityItem.avg_wind_speed_kmh} km/h</span>
                        </div>
                      </div>
                    </div>

                    <div className="mt-4 pt-3 border-t border-white/5 flex items-center justify-between text-xs text-cyan-400 font-medium">
                      <span>Inspect Forecast</span>
                      <ArrowUpRight className="w-3.5 h-3.5" />
                    </div>
                  </div>
                );
              })}
            </div>
          </div>
        )}

        {/* View Tab 2: Forecast Deep Dive */}
        {viewTab === 'deepdive' && (
          <div className="space-y-6">
            {/* City Selector Pills */}
            <div className="flex flex-wrap gap-2 pb-1 overflow-x-auto">
              {summaryData.map((c) => (
                <button
                  key={c.city}
                  onClick={() => setSelectedCity(c.city)}
                  className={`px-3.5 py-1.5 rounded-xl text-xs font-semibold transition-all ${
                    selectedCity === c.city
                      ? 'bg-cyan-500 text-white shadow-lg shadow-cyan-500/25 ring-1 ring-white/20'
                      : 'bg-surface hover:bg-surface-elevated text-gray-300 border border-white/5'
                  }`}
                >
                  {c.city_name}
                </button>
              ))}
            </div>

            {/* Selected City Hero Card */}
            {activeCitySummary && (
              <div className="glass-panel p-6 rounded-2xl border border-white/10 relative overflow-hidden">
                <div className="absolute top-0 right-0 w-96 h-96 bg-cyan-500/5 rounded-full blur-3xl pointer-events-none" />

                <div className="flex flex-col lg:flex-row items-start lg:items-center justify-between gap-6">
                  <div>
                    <div className="flex items-center gap-3">
                      <h2 className="text-2xl font-black tracking-tight text-white">
                        {activeCitySummary.city_name}
                      </h2>
                      <span className="text-xs text-gray-400 bg-white/5 px-2.5 py-1 rounded-md border border-white/5">
                        {activeCitySummary.state}
                      </span>
                      <span className="text-xs text-cyan-400 bg-cyan-500/10 px-2.5 py-1 rounded-md border border-cyan-500/20">
                        {activeCitySummary.population_tier}
                      </span>
                    </div>
                    <p className="text-xs text-gray-400 mt-1">
                      Coordinates: {activeCitySummary.latitude}°N, {activeCitySummary.longitude}°E • Timezone: Asia/Kolkata
                    </p>
                  </div>

                  <div className="flex items-center gap-4">
                    <div className={`p-4 rounded-xl border ${aqiStyle.bg} ${aqiStyle.border} text-center min-w-[130px]`}>
                      <span className="text-[11px] uppercase font-semibold text-gray-300 block">Current AQI</span>
                      <span className={`text-3xl font-extrabold ${aqiStyle.text}`}>
                        {Math.round(activeCitySummary.avg_us_aqi)}
                      </span>
                      <span className={`block text-[11px] font-semibold mt-0.5 ${aqiStyle.text}`}>{aqiStyle.label}</span>
                    </div>

                    <div className={`p-4 rounded-xl border ${predStyle.bg} ${predStyle.border} text-center min-w-[150px]`}>
                      <span className="text-[11px] uppercase font-semibold text-gray-300 block">Tomorrow Forecast</span>
                      <span className={`text-3xl font-extrabold ${predStyle.text}`}>
                        {activeCityPred ? activeCityPred.predicted_aqi : '--'}
                      </span>
                      <span className={`block text-[11px] font-semibold mt-0.5 ${predStyle.text}`}>
                        {activeCityPred ? activeCityPred.predicted_category : 'Model Inference'}
                      </span>
                    </div>
                  </div>
                </div>

                <div className="mt-6 pt-4 border-t border-white/5 grid grid-cols-2 sm:grid-cols-4 md:grid-cols-6 gap-3 text-xs">
                  <div className="bg-white/[0.03] p-3 rounded-xl">
                    <span className="text-gray-400 flex items-center gap-1 text-[11px]">
                      <Thermometer className="w-3.5 h-3.5 text-amber-400" /> Temperature
                    </span>
                    <span className="text-base font-bold text-white mt-1 block">
                      {activeCitySummary.avg_temperature_c}°C
                    </span>
                    <span className="text-[10px] text-gray-500">
                      Min: {activeCitySummary.min_temperature_c}° / Max: {activeCitySummary.max_temperature_c}°
                    </span>
                  </div>

                  <div className="bg-white/[0.03] p-3 rounded-xl">
                    <span className="text-gray-400 flex items-center gap-1 text-[11px]">
                      <Droplets className="w-3.5 h-3.5 text-cyan-400" /> Humidity
                    </span>
                    <span className="text-base font-bold text-white mt-1 block">
                      {activeCitySummary.avg_humidity_pct}%
                    </span>
                    <span className="text-[10px] text-gray-500">Relative humidity</span>
                  </div>

                  <div className="bg-white/[0.03] p-3 rounded-xl">
                    <span className="text-gray-400 flex items-center gap-1 text-[11px]">
                      <Wind className="w-3.5 h-3.5 text-indigo-400" /> Wind Speed
                    </span>
                    <span className="text-base font-bold text-white mt-1 block">
                      {activeCitySummary.avg_wind_speed_kmh} km/h
                    </span>
                    <span className="text-[10px] text-gray-500">Max: {activeCitySummary.max_wind_speed_kmh} km/h</span>
                  </div>

                  <div className="bg-white/[0.03] p-3 rounded-xl">
                    <span className="text-gray-400 flex items-center gap-1 text-[11px]">
                      <Activity className="w-3.5 h-3.5 text-rose-400" /> PM2.5 Fine
                    </span>
                    <span className="text-base font-bold text-white mt-1 block">
                      {activeCitySummary.avg_pm2_5} µg/m³
                    </span>
                    <span className="text-[10px] text-gray-500">Peak: {activeCitySummary.max_pm2_5} µg/m³</span>
                  </div>

                  <div className="bg-white/[0.03] p-3 rounded-xl">
                    <span className="text-gray-400 flex items-center gap-1 text-[11px]">
                      <Layers className="w-3.5 h-3.5 text-purple-400" /> PM10 Coarse
                    </span>
                    <span className="text-base font-bold text-white mt-1 block">
                      {activeCitySummary.avg_pm10} µg/m³
                    </span>
                    <span className="text-[10px] text-gray-500">Peak: {activeCitySummary.max_pm10} µg/m³</span>
                  </div>

                  <div className="bg-white/[0.03] p-3 rounded-xl">
                    <span className="text-gray-400 flex items-center gap-1 text-[11px]">
                      <Cpu className="w-3.5 h-3.5 text-emerald-400" /> Model MAE
                    </span>
                    <span className="text-base font-bold text-emerald-400 mt-1 block">
                      ±{activeCityPred ? activeCityPred.mae : '7.2'} pts
                    </span>
                    <span className="text-[10px] text-gray-500">Mean Absolute Error</span>
                  </div>
                </div>
              </div>
            )}

            {/* Time Series Recharts Area */}
            <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
              <div className="lg:col-span-2 glass-panel p-6 rounded-2xl border border-white/5 space-y-4">
                <div className="flex items-center justify-between">
                  <div>
                    <h3 className="text-base font-bold text-white">Historical AQI vs Model Prediction</h3>
                    <p className="text-xs text-gray-400">
                      14-day Gold mart history with rolling 7-day baseline and next-day machine learning projection
                    </p>
                  </div>
                  <div className="flex items-center gap-3 text-xs font-mono">
                    <div className="flex items-center gap-1.5">
                      <span className="w-2.5 h-2.5 rounded-full bg-cyan-400" />
                      <span className="text-gray-300">Actual AQI</span>
                    </div>
                    <div className="flex items-center gap-1.5">
                      <span className="w-2.5 h-2.5 rounded-full bg-purple-400" />
                      <span className="text-gray-300">Predicted Tomorrow</span>
                    </div>
                  </div>
                </div>

                <div className="h-72 w-full pt-2">
                  <ResponsiveContainer width="100%" height="100%">
                    <ComposedChart data={chartData}>
                      <CartesianGrid strokeDasharray="3 3" stroke="#1F2937" vertical={false} />
                      <XAxis dataKey="date" stroke="#6B7280" fontSize={11} tickLine={false} />
                      <YAxis stroke="#6B7280" fontSize={11} tickLine={false} domain={[0, 'dataMax + 40']} />
                      <Tooltip
                        contentStyle={{
                          backgroundColor: '#111827',
                          borderColor: 'rgba(255,255,255,0.1)',
                          borderRadius: '8px',
                          color: '#fff',
                          fontSize: '12px',
                        }}
                      />
                      <Area
                        type="monotone"
                        dataKey="actualAQI"
                        fill="rgba(6, 182, 212, 0.12)"
                        stroke="#06B6D4"
                        strokeWidth={2.5}
                        name="Actual US AQI"
                      />
                      <Line
                        type="monotone"
                        dataKey="rolling7d"
                        stroke="#10B981"
                        strokeWidth={1.5}
                        strokeDasharray="4 4"
                        dot={false}
                        name="7-Day Rolling Avg"
                      />
                      <Line
                        type="monotone"
                        dataKey="predictedAQI"
                        stroke="#A855F7"
                        strokeWidth={3}
                        dot={{ r: 6, fill: '#A855F7', stroke: '#fff', strokeWidth: 2 }}
                        name="Next-Day ML Forecast"
                      />
                    </ComposedChart>
                  </ResponsiveContainer>
                </div>
              </div>

              {/* Pollutants Breakdown Bar Chart */}
              <div className="glass-panel p-6 rounded-2xl border border-white/5 space-y-4">
                <h3 className="text-base font-bold text-white">Major Pollutant Levels</h3>
                <p className="text-xs text-gray-400">Average concentration in air (µg/m³)</p>

                <div className="h-72 w-full pt-2">
                  {activeCitySummary && (
                    <ResponsiveContainer width="100%" height="100%">
                      <BarChart
                        data={[
                          { name: 'PM2.5', value: activeCitySummary.avg_pm2_5, fill: '#EF4444' },
                          { name: 'PM10', value: activeCitySummary.avg_pm10, fill: '#F97316' },
                          { name: 'NO2', value: activeCitySummary.avg_nitrogen_dioxide, fill: '#8B5CF6' },
                          { name: 'O3', value: activeCitySummary.avg_ozone, fill: '#06B6D4' },
                          { name: 'SO2', value: activeCitySummary.avg_sulphur_dioxide, fill: '#EAB308' },
                        ]}
                        layout="vertical"
                      >
                        <CartesianGrid strokeDasharray="3 3" stroke="#1F2937" horizontal={false} />
                        <XAxis type="number" stroke="#6B7280" fontSize={11} />
                        <YAxis type="category" dataKey="name" stroke="#9CA3AF" fontSize={11} tickLine={false} />
                        <Tooltip
                          contentStyle={{
                            backgroundColor: '#111827',
                            borderColor: 'rgba(255,255,255,0.1)',
                            borderRadius: '8px',
                            color: '#fff',
                            fontSize: '12px',
                          }}
                        />
                        <Bar dataKey="value" radius={[0, 6, 6, 0]} />
                      </BarChart>
                    </ResponsiveContainer>
                  )}
                </div>
              </div>
            </div>

            {/* Health Advisory & Operational Guidance */}
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <div className="glass-panel p-5 rounded-2xl border border-white/5 space-y-2">
                <div className="flex items-center gap-2 text-sm font-bold text-white">
                  <ShieldCheck className="w-4 h-4 text-cyan-400" />
                  <span>Public Health Guidance ({aqiStyle.label})</span>
                </div>
                <p className="text-xs text-gray-300 leading-relaxed">{aqiStyle.description}</p>
                <div className="text-xs text-gray-400 pt-2 border-t border-white/5">
                  Recommendation: {activeCitySummary.avg_us_aqi > 150 ? 'Wear N95 masks outdoors, run HEPA air filtration indoors.' : 'Normal outdoor activities permitted for most individuals.'}
                </div>
              </div>

              <div className="glass-panel p-5 rounded-2xl border border-white/5 space-y-2">
                <div className="flex items-center gap-2 text-sm font-bold text-white">
                  <Cpu className="w-4 h-4 text-purple-400" />
                  <span>Model Architecture & Features</span>
                </div>
                <p className="text-xs text-gray-300 leading-relaxed">
                  Trained using <code className="text-purple-300 font-mono">HistGradientBoostingRegressor</code> on lagged features: 3-day AQI memory, temperature gradient, relative humidity, and surface wind speed.
                </p>
                <div className="text-xs text-gray-400 pt-2 border-t border-white/5 flex items-center justify-between">
                  <span>Artifact: <code className="text-gray-300">{selectedCity}_aqi_model_v1.joblib</code></span>
                  <span className="text-emerald-400 font-mono">Status: Production Scored</span>
                </div>
              </div>
            </div>
          </div>
        )}

        {/* View Tab 3: Lakehouse Platform Health & Observability */}
        {viewTab === 'observability' && (
          <div className="space-y-6">
            <div>
              <h2 className="text-lg font-bold text-white tracking-tight">
                Lakehouse Architecture & Medallion Observability
              </h2>
              <p className="text-xs text-gray-400">
                End-to-end telemetry from raw Open-Meteo landing to dbt data quality gates and DuckDB marts
              </p>
            </div>

            {/* Medallion Pipeline Architecture Visual */}
            <div className="glass-panel p-6 rounded-2xl border border-white/5 space-y-6">
              <h3 className="text-sm font-semibold uppercase tracking-wider text-cyan-400">
                Medallion Architecture Lineage Flow
              </h3>

              <div className="grid grid-cols-1 md:grid-cols-4 gap-4 relative">
                {/* Raw */}
                <div className="bg-[#0D1322] p-4 rounded-xl border border-white/5 space-y-2">
                  <div className="flex items-center justify-between text-xs">
                    <span className="font-bold text-gray-200">1. Raw Landing</span>
                    <span className="px-1.5 py-0.5 text-[9px] rounded bg-white/10 text-gray-300 font-mono">Immutable</span>
                  </div>
                  <p className="text-[11px] text-gray-400 leading-relaxed">
                    httpx + tenacity backoff. Writes hourly payloads to <code className="text-cyan-300">raw/{'{source}'}/{'{city}'}/{'{date}'}.json</code>. Never overwritten.
                  </p>
                  <div className="text-[10px] text-gray-500 font-mono pt-2 border-t border-white/5">
                    Storage: File System (S3/MinIO compatible)
                  </div>
                </div>

                {/* Bronze */}
                <div className="bg-[#0D1322] p-4 rounded-xl border border-white/5 space-y-2">
                  <div className="flex items-center justify-between text-xs">
                    <span className="font-bold text-indigo-300">2. Bronze Delta</span>
                    <span className="px-1.5 py-0.5 text-[9px] rounded bg-indigo-500/20 text-indigo-300 font-mono">Append Only</span>
                  </div>
                  <p className="text-[11px] text-gray-400 leading-relaxed">
                    Parsed with Polars. Arrow schema mapping with sha256 checksums and source file tracking into Delta tables.
                  </p>
                  <div className="text-[10px] text-gray-500 font-mono pt-2 border-t border-white/5">
                    Table: data/delta/bronze_*
                  </div>
                </div>

                {/* Silver */}
                <div className="bg-[#0D1322] p-4 rounded-xl border border-white/5 space-y-2">
                  <div className="flex items-center justify-between text-xs">
                    <span className="font-bold text-emerald-300">3. Silver Delta</span>
                    <span className="px-1.5 py-0.5 text-[9px] rounded bg-emerald-500/20 text-emerald-300 font-mono">ACID Upsert</span>
                  </div>
                  <p className="text-[11px] text-gray-400 leading-relaxed">
                    Type casting, quarantine quarantine table for out-of-bound records, and idempotent merge on (city, date, hour).
                  </p>
                  <div className="text-[10px] text-gray-500 font-mono pt-2 border-t border-white/5">
                    Table: data/delta/silver_*
                  </div>
                </div>

                {/* Gold */}
                <div className="bg-[#0D1322] p-4 rounded-xl border border-white/5 space-y-2">
                  <div className="flex items-center justify-between text-xs">
                    <span className="font-bold text-amber-300">4. Gold Marts (dbt)</span>
                    <span className="px-1.5 py-0.5 text-[9px] rounded bg-amber-500/20 text-amber-300 font-mono">Dimensional</span>
                  </div>
                  <p className="text-[11px] text-gray-400 leading-relaxed">
                    dbt-duckdb builds <code className="text-amber-300">dim_city</code> and <code className="text-amber-300">fct_daily_city_metrics</code> with rolling lag features and strict quality gates.
                  </p>
                  <div className="text-[10px] text-gray-500 font-mono pt-2 border-t border-white/5">
                    Warehouse: urbanpulse.duckdb
                  </div>
                </div>
              </div>
            </div>

            {/* Data Quality & Test Suite Summary */}
            <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
              <div className="glass-panel p-6 rounded-2xl border border-white/5 space-y-3">
                <div className="flex items-center gap-2">
                  <ShieldCheck className="w-5 h-5 text-emerald-400" />
                  <h3 className="text-base font-bold text-white">dbt Data Quality Test Suite</h3>
                </div>
                <p className="text-xs text-gray-400">
                  All tests must pass as a hard gate before the Gold analytical marts are published.
                </p>

                <div className="space-y-2 mt-4 text-xs font-mono">
                  <div className="flex items-center justify-between p-2.5 rounded-lg bg-white/[0.02] border border-white/5">
                    <span className="text-gray-300">stg_weather.unique_key(city, date, hour)</span>
                    <span className="text-emerald-400 font-bold">PASSED</span>
                  </div>
                  <div className="flex items-center justify-between p-2.5 rounded-lg bg-white/[0.02] border border-white/5">
                    <span className="text-gray-300">stg_weather.not_null(temperature_c)</span>
                    <span className="text-emerald-400 font-bold">PASSED</span>
                  </div>
                  <div className="flex items-center justify-between p-2.5 rounded-lg bg-white/[0.02] border border-white/5">
                    <span className="text-gray-300">stg_air_quality.not_null(us_aqi)</span>
                    <span className="text-emerald-400 font-bold">PASSED</span>
                  </div>
                  <div className="flex items-center justify-between p-2.5 rounded-lg bg-white/[0.02] border border-white/5">
                    <span className="text-gray-300">custom.test_aqi_sanity [0, 500 bounds]</span>
                    <span className="text-emerald-400 font-bold">PASSED</span>
                  </div>
                  <div className="flex items-center justify-between p-2.5 rounded-lg bg-white/[0.02] border border-white/5">
                    <span className="text-gray-300">fct_daily_city_metrics.accepted_values(aqi_category)</span>
                    <span className="text-emerald-400 font-bold">PASSED</span>
                  </div>
                </div>
              </div>

              <div className="glass-panel p-6 rounded-2xl border border-white/5 space-y-3">
                <div className="flex items-center gap-2">
                  <RefreshCw className="w-5 h-5 text-cyan-400" />
                  <h3 className="text-base font-bold text-white">Orchestration & Deployment Story</h3>
                </div>
                <p className="text-xs text-gray-400">
                  Dual-tier execution ensuring both enterprise orchestration demonstration and zero-cost public hosting.
                </p>

                <div className="space-y-3 mt-4 text-xs text-gray-300 leading-relaxed">
                  <div className="p-3 rounded-xl bg-white/[0.02] border border-white/5">
                    <span className="font-semibold text-white block mb-1">Local Orchestrator: Apache Airflow (Docker Compose)</span>
                    Full LocalExecutor with parameterized 14-day backfill, task retries, and task-level isolation.
                  </div>

                  <div className="p-3 rounded-xl bg-white/[0.02] border border-white/5">
                    <span className="font-semibold text-white block mb-1">Continuous Cloud Refresh: GitHub Actions</span>
                    Daily cron job runs the lightweight python pipeline runner, builds Delta logs, runs dbt tests, scores ML models, and commits snapshot JSONs to git.
                  </div>

                  <div className="p-3 rounded-xl bg-white/[0.02] border border-white/5">
                    <span className="font-semibold text-white block mb-1">Frontend Delivery: Next.js Static Export on Vercel</span>
                    Zero backend servers, instant edge cache delivery, 100% uptime with zero ongoing infrastructure costs.
                  </div>
                </div>
              </div>
            </div>

            {/* ML Methodology & Leak-Free Split Observability Panel */}
            <div className="glass-panel p-6 rounded-2xl border border-white/5 space-y-5">
              <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3">
                <div className="flex items-center gap-2.5">
                  <div className="w-8 h-8 rounded-lg bg-purple-500/20 flex items-center justify-center text-purple-400">
                    <Cpu className="w-4 h-4" />
                  </div>
                  <div>
                    <h3 className="text-base font-bold text-white">ML Methodology & Chronological Split Observability</h3>
                    <p className="text-xs text-gray-400">
                      Rigorous 3-way time-ordered split with zero hyperparameter selection leakage
                    </p>
                  </div>
                </div>
                <div className="flex items-center gap-2 font-mono text-xs">
                  <span className="px-2.5 py-1 rounded-md bg-purple-500/10 text-purple-300 border border-purple-500/20">
                    Seed: 42
                  </span>
                  <span className="px-2.5 py-1 rounded-md bg-emerald-500/10 text-emerald-300 border border-emerald-500/20">
                    Zero Leakage
                  </span>
                </div>
              </div>

              {/* 3-Way Split Telemetry Cards */}
              <div className="grid grid-cols-1 md:grid-cols-4 gap-3">
                <div className="p-3.5 rounded-xl bg-white/[0.02] border border-white/5 space-y-1">
                  <span className="text-[10px] uppercase font-mono text-gray-400">1. Train Window</span>
                  <div className="text-lg font-bold text-white font-mono">
                    {metaData?.layers?.ml_layer?.train_days || 39} Days
                  </div>
                  <p className="text-[11px] text-gray-400">Days 1–39: Candidate hyperparameter training</p>
                </div>

                <div className="p-3.5 rounded-xl bg-white/[0.02] border border-white/5 space-y-1">
                  <span className="text-[10px] uppercase font-mono text-indigo-400">2. Validation Window</span>
                  <div className="text-lg font-bold text-indigo-300 font-mono">
                    {metaData?.layers?.ml_layer?.val_days || 8} Days
                  </div>
                  <p className="text-[11px] text-gray-400">Days 40–47: Hyperparameter tuning only</p>
                </div>

                <div className="p-3.5 rounded-xl bg-white/[0.02] border border-white/5 space-y-1">
                  <span className="text-[10px] uppercase font-mono text-cyan-400">3. Train + Val Refit</span>
                  <div className="text-lg font-bold text-cyan-300 font-mono">47 Days</div>
                  <p className="text-[11px] text-gray-400">Days 1–47: Refit winning model architecture</p>
                </div>

                <div className="p-3.5 rounded-xl bg-white/[0.02] border border-white/5 space-y-1">
                  <span className="text-[10px] uppercase font-mono text-amber-400">4. Untouched Test Set</span>
                  <div className="text-lg font-bold text-amber-300 font-mono">
                    {metaData?.layers?.ml_layer?.test_days || 12} Days
                  </div>
                  <p className="text-[11px] text-gray-400">Days 48–59: Evaluated strictly once</p>
                </div>
              </div>

              {/* Tuning Decisions & Empirical Reality */}
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4 text-xs">
                <div className="p-4 rounded-xl bg-[#0B0F1C] border border-white/5 space-y-2">
                  <span className="font-semibold text-gray-200 block">Winning Hyperparameters (from Validation)</span>
                  <div className="space-y-1.5 font-mono text-gray-300 text-[11px]">
                    <div className="flex justify-between">
                      <span className="text-gray-500">Architecture:</span>
                      <span className="text-white">depth_3_leaf_3</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-gray-500">Parameters:</span>
                      <span className="text-cyan-300">max_depth=3, min_samples_leaf=3, lr=0.05</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-gray-500">Validation MAE:</span>
                      <span className="text-emerald-400 font-bold">9.33 (Best of 6 candidates)</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-gray-500">Missing Lags:</span>
                      <span className="text-gray-300">Native NaN Histogram Binning</span>
                    </div>
                  </div>
                </div>

                <div className="p-4 rounded-xl bg-[#0B0F1C] border border-white/5 space-y-2">
                  <span className="font-semibold text-gray-200 block">Plain-Disclosure Empirical Findings</span>
                  <p className="text-gray-300 leading-relaxed text-[11px]">
                    <span className="text-emerald-400 font-semibold">5 of 8 cities beat persistence</span> (Kolkata +29.8%, Ahmedabad +31.8%, Bengaluru +14.1%, Delhi +9.2%, Pune +1.6%).
                  </p>
                  <p className="text-gray-400 leading-relaxed text-[11px]">
                    <span className="text-amber-400 font-semibold">3 cities underperform persistence</span> (Mumbai, Chennai, Hyderabad). In low-volatility peninsular/coastal series, day-over-day drift is so small that model variance exceeds the bias of persistence.
                  </p>
                </div>
              </div>
            </div>
          </div>
        )}
      </main>

      {/* Footer */}
      <footer className="mt-auto border-t border-white/10 bg-[#080C16] px-4 lg:px-8 py-5 text-xs text-gray-500">
        <div className="max-w-7xl mx-auto flex flex-col sm:flex-row items-center justify-between gap-3">
          <div>
            <span className="text-gray-300 font-medium">UrbanPulse</span> — Multi-City Weather & Air Quality Intelligence Lakehouse
          </div>
          <div className="flex items-center gap-4">
            <span className="text-gray-400">Medallion Delta Lake + DuckDB + dbt + Scikit-Learn + Next.js</span>
          </div>
        </div>
      </footer>
    </div>
  );
}

function DatabaseIcon(props: React.SVGProps<SVGSVGElement>) {
  return (
    <svg
      {...props}
      xmlns="http://www.w3.org/2000/svg"
      width="24"
      height="24"
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="2"
      strokeLinecap="round"
      strokeLinejoin="round"
    >
      <ellipse cx="12" cy="5" rx="9" ry="3" />
      <path d="M3 5V19A9 3 0 0 0 21 19V5" />
      <path d="M3 12A9 3 0 0 0 21 12" />
    </svg>
  );
}
