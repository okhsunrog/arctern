// Shared Chart.js styling for the console: theme-aware colors pulled
// from the live CSS variables, no per-sample point markers, gradient
// area fills, quiet grids, mono tick labels. Rebuild options/data when
// the color mode flips — callers key their computeds on useColorMode.
import type { ScriptableContext, TooltipItem } from 'chart.js'

function cssVar(name: string): string {
  return getComputedStyle(document.documentElement).getPropertyValue(name).trim()
}

export function chartColors() {
  return {
    primary: cssVar('--ui-primary') || '#22d3ee',
    success: cssVar('--ui-success') || '#4ade80',
    error: cssVar('--ui-error') || '#f43f5e',
    text: cssVar('--ui-text-muted') || '#8b949e',
    grid: `color-mix(in oklab, ${cssVar('--ui-border') || '#30363d'} 55%, transparent)`,
    neutral: cssVar('--ui-text-dimmed') || '#6e7681',
    tooltipBg: cssVar('--ui-bg-elevated') || '#161b22',
    tooltipBorder: cssVar('--ui-border-accented') || '#3d444d',
  }
}

/** Vertical fade from `hex` at `alpha` down to transparent — the
 * scriptable form so the gradient tracks the chart's live height. */
export function areaGradient(color: string, alpha = 0.28) {
  return (ctx: ScriptableContext<'line'>) => {
    const { chartArea, ctx: canvas } = ctx.chart
    if (!chartArea) return 'transparent'
    const g = canvas.createLinearGradient(0, chartArea.top, 0, chartArea.bottom)
    // color-mix instead of hex-alpha concatenation: theme variables are
    // oklch() strings, not hex.
    g.addColorStop(0, `color-mix(in srgb, ${color} ${Math.round(alpha * 100)}%, transparent)`)
    g.addColorStop(1, `color-mix(in srgb, ${color} 0%, transparent)`)
    return g
  }
}

const MONO = "'JetBrains Mono', ui-monospace, monospace"

export function lineDataset(overrides: Record<string, unknown>) {
  return {
    borderWidth: 2,
    pointRadius: 0,
    pointHoverRadius: 3.5,
    pointHitRadius: 12,
    tension: 0.35,
    cubicInterpolationMode: 'monotone' as const,
    ...overrides,
  }
}

export function baseOptions(opts: {
  yTick?: (v: number) => string
  tooltipValue?: (item: TooltipItem<'line' | 'bar'>) => string
}) {
  const c = chartColors()
  return {
    responsive: true,
    maintainAspectRatio: false,
    animation: { duration: 250 },
    interaction: { mode: 'index' as const, intersect: false },
    plugins: {
      legend: { display: false },
      tooltip: {
        backgroundColor: c.tooltipBg,
        borderColor: c.tooltipBorder,
        borderWidth: 1,
        titleColor: c.text,
        titleFont: { family: MONO, size: 11, weight: 'normal' as const },
        bodyFont: { family: MONO, size: 12 },
        padding: 10,
        cornerRadius: 6,
        displayColors: true,
        boxWidth: 8,
        boxHeight: 8,
        boxPadding: 4,
        usePointStyle: true,
        callbacks: opts.tooltipValue ? { label: opts.tooltipValue } : undefined,
      },
    },
    scales: {
      x: {
        grid: { display: false },
        border: { color: c.grid },
        ticks: {
          color: c.text,
          font: { family: MONO, size: 10 },
          maxTicksLimit: 7,
          maxRotation: 0,
          autoSkip: true,
        },
      },
      y: {
        grid: { color: c.grid, drawTicks: false },
        border: { display: false },
        ticks: {
          color: c.text,
          font: { family: MONO, size: 10 },
          maxTicksLimit: 6,
          padding: 8,
          callback: opts.yTick
            ? (v: string | number) => opts.yTick!(typeof v === 'number' ? v : Number(v))
            : undefined,
        },
      },
    },
  }
}
