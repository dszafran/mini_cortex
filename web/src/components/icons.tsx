const base = {
  viewBox: '0 0 20 20',
  fill: 'none',
  stroke: 'currentColor',
  strokeWidth: 1.6,
  strokeLinecap: 'round' as const,
  strokeLinejoin: 'round' as const,
  'aria-hidden': true,
}

export const ChatIcon = () => (
  <svg {...base}>
    <path d="M3.5 5.5a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v6a2 2 0 0 1-2 2H9l-3.5 3v-3a2 2 0 0 1-2-2z" />
  </svg>
)
export const MemoryIcon = () => (
  <svg {...base}>
    <path d="M10 3 17 6.5 10 10 3 6.5z" />
    <path d="m3 10 7 3.5 7-3.5M3 13.5 10 17l7-3.5" />
  </svg>
)
export const SourcesIcon = () => (
  <svg {...base}>
    <path d="M7 3v4M13 3v4M5 7h10v3a5 5 0 0 1-10 0zM10 15v2.5" />
  </svg>
)
export const TuneIcon = () => (
  <svg {...base}>
    <path d="M4 6h7M15 6h1M4 14h1M9 14h7" />
    <circle cx="13" cy="6" r="2" />
    <circle cx="7" cy="14" r="2" />
  </svg>
)
export const SearchIcon = () => (
  <svg {...base}>
    <circle cx="9" cy="9" r="5" />
    <path d="m13 13 3.5 3.5" />
  </svg>
)
export const FetchIcon = () => (
  <svg {...base}>
    <path d="M10 3v9M6 8.5 10 12.5l4-4M4 16h12" />
  </svg>
)
export const SunIcon = () => (
  <svg {...base}>
    <circle cx="10" cy="10" r="3.2" />
    <path d="M10 2.5v1.8M10 15.7v1.8M2.5 10h1.8M15.7 10h1.8M4.7 4.7l1.3 1.3M14 14l1.3 1.3M4.7 15.3 6 14M14 6l1.3-1.3" />
  </svg>
)
export const MoonIcon = () => (
  <svg {...base}>
    <path d="M15.5 12.5A6 6 0 0 1 7.5 4.5a6 6 0 1 0 8 8z" />
  </svg>
)
export const SendIcon = () => (
  <svg {...base}>
    <path d="M4 10h11M11 5.5 15.5 10 11 14.5" />
  </svg>
)
