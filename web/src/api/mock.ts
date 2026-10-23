// Demo data (a small telco story) shown when no app is running behind the UI.
import type {
  ChatMessage,
  Citation,
  ConnectorInfo,
  DoctorCheck,
  Lens,
  LlmModel,
  LlmSettings,
  MemoryLogEntry,
  Packet,
  PipelineStats,
  Settings,
  Thread,
  ToolStep,
  WireFrame,
} from './types'

const src = (
  connector: string,
  stream: string,
  timestamp: string,
  author?: string,
  label?: string,
) => ({
  source_id: `${connector}:${stream}:${timestamp}`,
  connector,
  stream,
  timestamp,
  author,
  label,
})

const packet = (p: Partial<Packet> & Pick<Packet, 'id' | 'type' | 'title' | 'content' | 'sources'>): Packet => ({
  entities: [],
  source_timestamp: p.sources[0].timestamp,
  created_at: '2026-10-18T10:30:00+02:00',
  updated_at: '2026-10-18T10:30:00+02:00',
  deprecated: false,
  superseded_by: null,
  access_count: 0,
  ...p,
})

export const packets: Packet[] = [
  packet({
    id: 'pk_0388',
    type: 'decision',
    title: 'Harbour small cell + 2nd 1800 MHz carrier, live by 30 Oct',
    content:
      "The COW trailer is cancelled because it does not fit on the ferry. Herring Days capacity comes from a small cell on the harbour master's roof, live by 30 Oct, plus a second 1800 MHz carrier on the lighthouse. Decided by Agnes.",
    entities: ['Herring Days', 'harbour small cell', 'Agnes Muir'],
    sources: [src('mattermost', 'network', '2026-10-04T11:20:00+02:00', 'agnes.muir')],
    access_count: 7,
  }),
  packet({
    id: 'pk_0301',
    type: 'decision',
    title: "Rent Mainline's COW trailer for Herring Days",
    content:
      "Rent Mainline's Cell-on-Wheels trailer, parked at the harbour car park for Herring Days (31 Oct – 2 Nov), arriving on the 30 Oct ferry. Fastest capacity, no permits.",
    entities: ['Herring Days', 'COW trailer', 'Mainline Telecom'],
    sources: [
      src('mattermost', 'network', '2026-09-26T15:02:00+02:00', 'maren.coyle'),
      src('notes', 'notes', '2026-09-26T18:40:00+02:00', undefined, 'work/meetings/herring-days-planning.md'),
    ],
    deprecated: true,
    superseded_by: 'pk_0388',
  }),
  packet({
    id: 'pk_0402',
    type: 'event',
    title: 'Small cell install 26–28 Oct, testing 29 Oct, live 30 Oct',
    content: 'Harbour small cell: install 26–28 Oct, testing 29 Oct, live 30 Oct. Harbour master permission granted 13 Oct.',
    entities: ['harbour small cell'],
    sources: [src('mattermost', 'network', '2026-10-17T09:12:00+02:00', 'maren.coyle')],
    access_count: 3,
  }),
  packet({
    id: 'pk_0403',
    type: 'warning',
    title: 'Change freeze 31 Oct – 4 Nov',
    content: 'No network changes from 31 Oct to 4 Nov, covering Herring Days and the day after.',
    entities: ['Herring Days', 'change freeze'],
    sources: [src('mattermost', 'network', '2026-10-17T09:12:00+02:00', 'maren.coyle')],
  }),
  packet({
    id: 'pk_0431',
    type: 'fact',
    title: 'Mounting pole missing; reordered for the 24 Oct freight ferry',
    content:
      "The small cell kit arrived on Tuesday's freight ferry without its mounting pole. The pole was reordered on the 24 Oct freight sailing.",
    entities: ['harbour small cell', 'Deniz Aksoy'],
    sources: [src('mattermost', 'field-ops', '2026-10-21T08:05:00+02:00', 'deniz.aksoy')],
  }),
  packet({
    id: 'pk_0417',
    type: 'fact',
    title: 'Gull nest knocked the Halsby microwave dish ~2° off alignment',
    content:
      "On 14 Oct a herring gull's nest (Gertrude) pushed the Halsby microwave dish about 2° off alignment and the link margin dropped to zero. Lessons: inspect the dish monthly, fit gull spikes on all dish mounts before nesting season, never climb in winds above 40 km/h.",
    entities: ['Gertrude', 'Halsby link', 'microwave dish', 'Bryony Kett'],
    sources: [
      src('mattermost', 'incidents', '2026-10-14T08:30:00+02:00', 'bryony.kett'),
      src('mail', 'inbox', '2026-10-16T17:05:00+02:00', 'Bryony Kett', 'Postmortem: 14 Oct backhaul outage'),
    ],
    updated_at: '2026-10-18T10:42:07+02:00',
    access_count: 4,
  }),
  packet({
    id: 'pk_0418',
    type: 'event',
    title: 'Backhaul outage 14 Oct: 4 h 05 min, ~3,100 customers',
    content: 'Backhaul to Halsby down from 06:14 to 10:19 on 14 Oct. Island-wide, about 3,100 customers. Deniz realigned the dish.',
    entities: ['Halsby link', 'Deniz Aksoy'],
    sources: [src('mattermost', 'incidents', '2026-10-14T10:22:00+02:00', 'lachlan.reid')],
  }),
  packet({
    id: 'pk_0420',
    type: 'fact',
    title: 'CR-812: re-align and re-torque the dish mount',
    content: 'Change request CR-812: re-align and re-torque the microwave dish mount at Halsby.',
    entities: ['CR-812', 'microwave dish'],
    sources: [src('mattermost', 'field-ops', '2026-10-17T14:10:00+02:00', 'deniz.aksoy')],
  }),
  packet({
    id: 'pk_0425',
    type: 'fact',
    title: 'CR-815: gull spikes on sector-2 800 MHz panels',
    content: 'Change request CR-815: fit gull spikes on the 800 MHz antenna panels, sector 2.',
    entities: ['CR-815', 'antenna panels'],
    sources: [src('mattermost', 'field-ops', '2026-10-18T10:41:00+02:00', 'bryony.kett')],
  }),
  packet({
    id: 'pk_0360',
    type: 'decision',
    title: 'Buying the SkyTether satellite backup: SLA over latency',
    content: 'Lighthouse Mobile buys the SkyTether GEO satellite backhaul backup. The SLA matters more than the latency. Decided by Agnes.',
    entities: ['SkyTether', 'backhaul backup', 'Agnes Muir'],
    sources: [src('mattermost', 'general', '2026-10-15T12:00:00+02:00', 'agnes.muir')],
  }),
  packet({
    id: 'pk_0340',
    type: 'fact',
    title: 'SkyTether vs Orbitwave: price, speed, SLA',
    content:
      'SkyTether (GEO): €2,900 install + €1,150/month, 100/20 Mbps, ~600 ms latency, with an SLA. Orbitwave (LEO): €690/month, ~40 ms, no SLA.',
    entities: ['SkyTether', 'Orbitwave'],
    sources: [src('notes', 'notes', '2026-10-01T21:15:00+02:00', undefined, 'work/backhaul-backup-options.md')],
  }),
  packet({
    id: 'pk_0362',
    type: 'warning',
    title: 'VoLTE calls fail over GEO satellite',
    content: 'Over GEO satellite, VoLTE calls fail because latency breaks call setup. In failover only data and SMS survive. The shop needs a script for customers.',
    entities: ['SkyTether', 'VoLTE', 'Bryony Kett'],
    sources: [src('mattermost', 'network', '2026-10-12T16:45:00+02:00', 'bryony.kett')],
  }),
  packet({
    id: 'pk_0366',
    type: 'commitment',
    title: 'Sign and send the SkyTether PO by 25 Oct',
    content: 'Maren promised Agnes to sign the SkyTether purchase order and send it by 25 Oct, so the kit makes the 28 Oct freight ferry.',
    entities: ['SkyTether', 'Agnes Muir'],
    sources: [
      src('mattermost', 'dm', '2026-10-15T12:10:00+02:00', 'agnes.muir'),
      src('notes', 'notes', '2026-10-18T07:30:00+02:00', undefined, 'work/todo.md'),
    ],
  }),
  packet({
    id: 'pk_0372',
    type: 'preference',
    title: 'Gregor wants traffic reports as CSV by email',
    content: 'Gregor Haldane (Mainline) wants traffic reports as CSV by email, never PDF. No calls before 10:00.',
    entities: ['Gregor Haldane', 'Mainline Telecom'],
    sources: [src('mail', 'inbox', '2026-09-22T10:05:00+02:00', 'Gregor Haldane', 'Re: September traffic report')],
  }),
  packet({
    id: 'pk_0375',
    type: 'person',
    title: 'Fenella Dunmore signs the Mainline roaming renewal',
    content: "Fenella Dunmore is Mainline Telecom's commercial director and signs the roaming renewal. Gregor runs it day to day.",
    entities: ['Fenella Dunmore', 'Gregor Haldane', 'Mainline Telecom'],
    sources: [src('mattermost', 'network', '2026-10-02T13:30:00+02:00', 'saoirse.vale')],
  }),
  packet({
    id: 'pk_0310',
    type: 'reference',
    title: "Last year's Herring Days postmortem",
    content:
      'wiki.lighthouse-mobile.example/herring-days-postmortem: 2,900 simultaneous users and an 18% call-drop rate last year.',
    entities: ['Herring Days', 'Bryony Kett'],
    sources: [src('mattermost', 'network', '2026-09-13T10:00:00+02:00', 'bryony.kett')],
  }),
]

export const memoryLog: MemoryLogEntry[] = [
  {
    id: 'ml_9012',
    at: '2026-10-18T10:42:07+02:00',
    stage: 'dedup',
    action: 'MERGE',
    title: 'Gull nest knocked the Halsby dish ~2° off alignment',
    reason:
      'Same incident and the same root cause as the #incidents post. The email adds the prevention lessons. CR-812 is a follow-up task, not the same fact.',
    packet_ids: ['pk_0417'],
    source: src('mail', 'inbox', '2026-10-16T17:05:00+02:00', 'Bryony Kett', 'Postmortem: 14 Oct backhaul outage'),
    detail: {
      target_id: 'pk_0417',
      neighbours: [
        { packet_id: 'pk_0417', title: "Gertrude's nest pushed the dish off (#incidents)", cosine: 0.91 },
        { packet_id: 'pk_0420', title: 'CR-812: re-align and re-torque dish mount', cosine: 0.88 },
        { packet_id: 'pk_0418', title: 'Outage 4 h 05 min, ~3,100 customers', cosine: 0.74 },
      ],
      content_before:
        "On 14 Oct a herring gull's nest (Gertrude) pushed the Halsby microwave dish about 2° off alignment and the link margin dropped to zero.",
      content_after:
        " Lessons: inspect the dish monthly, fit gull spikes on all dish mounts before nesting season, never climb in winds above 40 km/h.",
    },
  },
  {
    id: 'ml_9011',
    at: '2026-10-18T10:42:05+02:00',
    stage: 'packetize',
    action: 'PACKETIZE',
    title: '3 packets from "Postmortem: 14 Oct backhaul outage"',
    reason: 'Root cause, prevention lessons and the climbing wind limit are durable. The greeting and sign-off were dropped.',
    packet_ids: ['pk_0417', 'pk_0418', 'pk_0420'],
    source: src('mail', 'inbox', '2026-10-16T17:05:00+02:00', 'Bryony Kett', 'Postmortem: 14 Oct backhaul outage'),
    detail: { produced: ['pk_0417', 'pk_0418', 'pk_0420'] },
  },
  {
    id: 'ml_9010',
    at: '2026-10-18T10:41:58+02:00',
    stage: 'dedup',
    action: 'ADD',
    title: 'CR-815: gull spikes on sector-2 800 MHz panels',
    reason: 'Close to CR-812 (0.89), but it is a different change request on different hardware.',
    packet_ids: ['pk_0425'],
    source: src('mattermost', 'field-ops', '2026-10-18T10:41:00+02:00', 'bryony.kett'),
    detail: {
      neighbours: [
        { packet_id: 'pk_0420', title: 'CR-812: re-align and re-torque dish mount', cosine: 0.89 },
        { packet_id: 'pk_0417', title: 'Gull nest knocked the Halsby dish off', cosine: 0.81 },
      ],
    },
  },
  {
    id: 'ml_9009',
    at: '2026-10-18T10:41:51+02:00',
    stage: 'packetize',
    action: 'SKIP',
    title: 'TowerTalk Weekly: 5 antenna trends',
    reason: 'Newsletter. It also contains instructions addressed to an AI assistant; treated as data only.',
    packet_ids: [],
    source: src('mail', 'inbox', '2026-10-21T07:00:00+02:00', 'digest@towertalk-weekly.example', 'TowerTalk Weekly: 5 antenna trends'),
  },
  {
    id: 'ml_9008',
    at: '2026-10-18T10:41:40+02:00',
    stage: 'intake',
    action: 'PUSH',
    title: '1 item from mattermost #field-ops',
    reason: 'Deniz Aksoy: "pole arrived on the freight ferry 🎉 install still 26 Oct"',
    packet_ids: [],
    source: src('mattermost', 'field-ops', '2026-10-18T10:41:39+02:00', 'deniz.aksoy'),
    detail: { items: 1 },
  },
  {
    id: 'ml_9007',
    at: '2026-10-18T10:41:22+02:00',
    stage: 'dedup',
    action: 'SKIP',
    title: 'Small cell live by 30 Oct',
    reason: 'Already in memory with the same facts.',
    packet_ids: ['pk_0402'],
    source: src('notes', 'notes', '2026-10-17T21:00:00+02:00', undefined, 'work/herring-days-capacity.md'),
    detail: {
      target_id: 'pk_0402',
      neighbours: [{ packet_id: 'pk_0402', title: 'Small cell install 26–28 Oct, live 30 Oct', cosine: 0.95 }],
    },
  },
  {
    id: 'ml_9006',
    at: '2026-10-18T10:40:58+02:00',
    stage: 'intake',
    action: 'FETCH',
    title: '50 items from mattermost #network',
    packet_ids: [],
    source: src('mattermost', 'network', '2026-10-18T10:40:58+02:00'),
    detail: { items: 50, page: 'page 3 of 7 · 1 Sep → 18 Oct' },
  },
  {
    id: 'ml_9005',
    at: '2026-10-18T10:40:31+02:00',
    stage: 'dedup',
    action: 'ADD',
    title: 'VoLTE calls fail over GEO satellite',
    reason: 'No packet above θ. Nothing similar in memory yet.',
    packet_ids: ['pk_0362'],
    source: src('mattermost', 'network', '2026-10-12T16:45:00+02:00', 'bryony.kett'),
    detail: {
      neighbours: [{ packet_id: 'pk_0340', title: 'SkyTether vs Orbitwave: price, speed, SLA', cosine: 0.71 }],
    },
  },
  {
    id: 'ml_9004',
    at: '2026-10-18T10:40:12+02:00',
    stage: 'packetize',
    action: 'SKIP',
    title: 'nms-bot: sector 1 VSWR warning cleared',
    reason: 'Automated alarm that cleared itself. Nothing durable.',
    packet_ids: [],
    source: src('mattermost', 'network', '2026-10-11T03:12:00+02:00', 'nms-bot'),
  },
  {
    id: 'ml_9003',
    at: '2026-10-18T10:39:47+02:00',
    stage: 'dedup',
    action: 'MERGE',
    title: 'Sign and send the SkyTether PO by 25 Oct',
    reason: 'The todo note and the DM describe the same promise to Agnes with the same deadline.',
    packet_ids: ['pk_0366'],
    source: src('notes', 'notes', '2026-10-18T07:30:00+02:00', undefined, 'work/todo.md'),
    detail: {
      target_id: 'pk_0366',
      neighbours: [{ packet_id: 'pk_0366', title: 'Sign and send the SkyTether PO by 25 Oct', cosine: 0.93 }],
      content_before:
        'Maren promised Agnes to sign the SkyTether purchase order and send it by 25 Oct',
      content_after: ', so the kit makes the 28 Oct freight ferry.',
    },
  },
]

export const stats: PipelineStats = {
  intake: 412,
  streams: 3,
  packetized: 160,
  nothing_durable: 252,
  merged: 21,
  skipped_known: 11,
  packets: 128,
  types: 8,
}

export const connectors: ConnectorInfo[] = [
  {
    name: 'mattermost',
    label: 'Mattermost',
    version: '1',
    status: 'live',
    detail: 'lighthouse-mobile',
    connected_since: '2026-10-18T10:27:02+02:00',
    streams: [
      { name: '#network', supports: ['fetch', 'push'], items: 150, backfill: { done: 150, total: 348 } },
      { name: '#incidents', supports: ['fetch', 'push'], items: 61, backfill: { done: 61, total: 61 } },
      { name: '#field-ops', supports: ['fetch', 'push'], items: 84, backfill: { done: 84, total: 84 } },
      { name: '#shop', supports: ['fetch', 'push'], items: 52, backfill: { done: 52, total: 52 } },
      { name: 'DMs (archive)', supports: ['fetch'], items: 27, backfill: { done: 27, total: 27 } },
    ],
  },
  {
    name: 'notes',
    label: 'Notes folder',
    version: '1',
    status: 'live',
    detail: '~/mini-cortex/notes · watching for saves',
    connected_since: '2026-10-18T10:26:40+02:00',
    streams: [{ name: 'notes', supports: ['fetch', 'push'], items: 38, backfill: { done: 38, total: 38 } }],
  },
  {
    name: 'replay',
    label: 'Replay',
    version: '1',
    status: 'idle',
    detail: 'fixtures/lighthouse.jsonl · offline fallback, not started',
    streams: [],
  },
]

export const hubAddress = 'ws://localhost:8765/connectors'

export const frames: WireFrame[] = [
  {
    id: 'f1',
    at: '2026-10-18T10:27:02+02:00',
    dir: 'in',
    method: 'hello',
    summary: 'mattermost v1 · 5 streams',
    payload: {
      jsonrpc: '2.0',
      id: 'h-1',
      method: 'hello',
      params: {
        connector: 'mattermost',
        version: '1',
        streams: [
          { name: 'network', supports: ['fetch', 'push'] },
          { name: 'incidents', supports: ['fetch', 'push'] },
          { name: 'field-ops', supports: ['fetch', 'push'] },
          { name: 'shop', supports: ['fetch', 'push'] },
          { name: 'dm', supports: ['fetch'] },
        ],
      },
    },
  },
  {
    id: 'f2',
    at: '2026-10-18T10:27:02+02:00',
    dir: 'out',
    method: 'fetch',
    summary: '#network · 1 Sep → 18 Oct · limit 50',
    payload: {
      jsonrpc: '2.0',
      id: 7,
      method: 'fetch',
      params: { stream: 'network', since: '2026-09-01T00:00:00Z', until: '2026-10-18T08:27:00Z', cursor: null, limit: 50 },
    },
  },
  {
    id: 'f3',
    at: '2026-10-18T10:27:03+02:00',
    dir: 'in',
    method: 'result',
    summary: '50 items · has_more · cursor "p2"',
    payload: { jsonrpc: '2.0', id: 7, result: { items: ['… 50 IngestItems …'], next_cursor: 'p2', has_more: true } },
  },
  {
    id: 'f4',
    at: '2026-10-18T10:41:40+02:00',
    dir: 'in',
    method: 'push',
    summary: '#field-ops · 1 item · id c-12',
    payload: {
      jsonrpc: '2.0',
      id: 'c-12',
      method: 'push',
      params: {
        stream: 'field-ops',
        items: [
          {
            source_id: 'mattermost:field-ops:8k2p…',
            content: 'pole arrived on the freight ferry 🎉 install still 26 Oct',
            timestamp: '2026-10-18T08:41:39Z',
            metadata: { author: 'deniz.aksoy' },
          },
        ],
      },
    },
  },
  {
    id: 'f5',
    at: '2026-10-18T10:41:40+02:00',
    dir: 'out',
    method: 'ack',
    summary: 'c-12 · accepted 1',
    payload: { jsonrpc: '2.0', id: 'c-12', result: { accepted: 1 } },
  },
]

export const lenses: Lens[] = [
  { id: 'work', name: 'Work me', intent: 'Decisions, outages, deadlines, vendor facts, risks.' },
  { id: 'personal', name: 'Personal me', intent: 'Family, home, money, plans and promises.' },
  { id: 'health', name: 'Health & life coach', intent: 'Sleep, stress, exercise, medical advice, overload.' },
]

export const defaultSettings: Settings = {
  lens: 'work',
  theta: 0.86,
  buffer: { messages: 5, seconds: 20 },
  types: [
    { name: 'fact', definition: 'Durable fact or context', enabled: true },
    { name: 'event', definition: 'Happens at a specific time', enabled: true },
    { name: 'decision', definition: 'A choice, with its reason', enabled: true },
    { name: 'person', definition: 'Who someone is to you', enabled: true },
    { name: 'preference', definition: 'Likes, habits, constraints', enabled: true },
    { name: 'reference', definition: 'A link or document and its use', enabled: true },
    { name: 'warning', definition: 'Risk or gotcha', enabled: true },
    { name: 'commitment', definition: 'A promise with a due date (stretch)', enabled: false },
  ],
}

export const threads: Thread[] = [
  { id: 't1', title: 'Herring Days plan', updated: 'just now' },
  { id: 't2', title: 'Backhaul backup summary', updated: '10:12' },
  { id: 't3', title: 'What did I promise this week?', updated: '09:58' },
  { id: 't4', title: "Gregor's report format", updated: 'yesterday' },
]

const search = (args: string, result: string): ToolStep => ({ tool: 'search_knowledge', args, result })
const fetchStep = (args: string, result: string): ToolStep => ({ tool: 'fetch_from_source', args, result })

const pc = (n: number, id: string, where: string, when: string): Citation => {
  const p = packets.find((x) => x.id === id)!
  return { n, kind: 'packet', title: p.title, type: p.type, where, when, packet_id: id }
}

export const initialMessages: Record<string, ChatMessage[]> = {
  t1: [
    { id: 'm1', role: 'user', content: "What's the capacity plan for Herring Days?" },
    {
      id: 'm2',
      role: 'assistant',
      steps: [search('"Herring Days capacity plan"', 'hybrid · 6 hits · 140 ms')],
      content:
        "The plan is a **harbour small cell** on the harbour master's roof, live by **30 Oct** (install 26–28 Oct), plus a **second 1800 MHz carrier** on the lighthouse [1][2]. Changes are frozen 31 Oct – 4 Nov [3].\n\nOne open risk: the mounting pole was missing from the kit. It was reordered on the 24 Oct freight sailing [4].",
      citations: [
        pc(1, 'pk_0388', '#network', '4 Oct'),
        pc(2, 'pk_0402', '#network', '17 Oct'),
        pc(3, 'pk_0403', '#network', '17 Oct'),
        pc(4, 'pk_0431', '#field-ops', '21 Oct'),
      ],
      superseded: [{ title: "rent Mainline's COW trailer", note: 'decided 26 Sep, replaced 4 Oct' }],
    },
    { id: 'm3', role: 'user', content: 'What happened in #incidents last week?' },
    {
      id: 'm4',
      role: 'assistant',
      steps: [
        search('"incidents last week"', '3 hits'),
        fetchStep('mattermost · #incidents · 12 → 18 Oct', '9 raw items'),
      ],
      content:
        "One real incident. On 14 Oct at 06:14 the backhaul to Halsby went down island-wide. Gertrude's nest had pushed the microwave dish about 2° out of alignment [5]. Deniz realigned it at 10:19, after 4 h 05 min and about 3,100 affected customers [6]. The rest was bot noise.",
      citations: [
        { n: 5, kind: 'raw', title: 'Bryony: "Yes. It\'s Gertrude."', where: '#incidents', when: '14 Oct 08:30' },
        { n: 6, kind: 'raw', title: 'Lachlan: link up 10:19, outage 4 h 05 min', where: '#incidents', when: '14 Oct 10:22' },
      ],
    },
  ],
  t2: [],
  t3: [],
  t4: [],
}

interface Canned {
  match: RegExp
  steps: ToolStep[]
  text: string
  citations: Citation[]
  superseded?: { title: string; note: string }[]
}

export const canned: Canned[] = [
  {
    match: /backhaul|satellite|skytether|orbitwave/i,
    steps: [search('"backhaul backup satellite"', 'hybrid · 9 hits · 160 ms')],
    text:
      "You're buying the **SkyTether** satellite backup [1]. It costs €2,900 to install plus €1,150/month, for 100/20 Mbps at ~600 ms latency. Orbitwave was cheaper at €690/month but has no SLA, and the SLA won [2]. One catch: over GEO satellite **VoLTE calls fail**, so in failover only data and SMS work [3]. You promised Agnes the PO by **25 Oct** [4].",
    citations: [
      pc(1, 'pk_0360', '#general', '15 Oct'),
      pc(2, 'pk_0340', 'notes', '1 Oct'),
      pc(3, 'pk_0362', '#network', '12 Oct'),
      pc(4, 'pk_0366', 'DM · Agnes', '15 Oct'),
    ],
  },
  {
    match: /promise|commit|owe|todo/i,
    steps: [search('"commitments this week"', 'hybrid · 4 hits · 120 ms')],
    text: 'One commitment is due this week: sign and send the **SkyTether PO by 25 Oct**, so the kit makes the 28 Oct freight ferry [1].',
    citations: [pc(1, 'pk_0366', 'DM · Agnes', '15 Oct')],
  },
  {
    match: /cancel/i,
    steps: [search('"harbour small cell cancelled"', 'hybrid · 5 hits · 130 ms')],
    text:
      "No. The harbour small cell is still planned to go live on **30 Oct** [1][2]. A newsletter in your inbox contained instructions aimed at an AI assistant that claimed otherwise. I treated it as data and ignored it.",
    citations: [pc(1, 'pk_0388', '#network', '4 Oct'), pc(2, 'pk_0402', '#network', '17 Oct')],
  },
  {
    match: /gregor|report|csv/i,
    steps: [search('"Gregor traffic reports format"', 'hybrid · 3 hits · 110 ms')],
    text: 'Send Gregor the traffic reports as **CSV by email**, never PDF, and don\'t call him before 10:00 [1]. Fenella Dunmore signs the renewal itself [2].',
    citations: [pc(1, 'pk_0372', 'mail', '22 Sep'), pc(2, 'pk_0375', '#network', '2 Oct')],
  },
]

export const fallback: Canned = {
  match: /.*/,
  steps: [search('(your question)', 'hybrid · 0 relevant hits')],
  text:
    "I searched memory and found nothing relevant yet. Try asking about Herring Days, the backhaul backup, the 14 Oct outage or what you've promised this week.",
  citations: [],
}

// ---------- models & keys (demo mode only; live mode talks to /api/llm) ----------

const presets: LlmSettings['presets'] = [
  {
    id: 'scaleway',
    label: 'Scaleway Generative APIs',
    base_url: 'https://api.scaleway.ai/<project id>/v1',
    needs_key: true,
    key_env: 'SCW_SECRET_KEY',
    key_help: "Scaleway console → IAM → API keys. The key's policy needs Generative APIs access in this project.",
    chat_model: 'glm-5.3',
    extract_model: 'deepseek-v4.1-flash',
    embed_model: 'qwen3-embedding-8b',
    embed_dim: 1024,
  },
]

export const llmSettings: LlmSettings = {
  provider: 'scaleway',
  base_url: 'https://api.scaleway.ai/v1',
  project_id: '',
  key_set: true,
  key_hint: '••••demo',
  chat_model: 'glm-5.3',
  extract_model: 'deepseek-v4.1-flash',
  embed_model: 'qwen3-embedding-8b',
  embed_dim: 1024,
  ca_bundle: '',
  missing: [],
  env_file: '.env',
  overridden: [],
  presets,
}

export const llmModels: LlmModel[] = [
  'deepseek-v4.1-flash',
  'glm-5.3',
  'mistral-medium-3.5-128b',
  'qwen3-235b-a22b-instruct-2507',
  'qwen3.5-397b-a17b',
].map((id): LlmModel => ({ id, kind: 'chat' })).concat([
  { id: 'bge-multilingual-gemma2', kind: 'embed' },
  { id: 'qwen3-embedding-8b', kind: 'embed' },
])

export const doctorChecks: DoctorCheck[] = [
  { name: 'settings', status: 'ok', detail: 'demo data: no app running, nothing was called', fix: '', ms: null },
  { name: 'models', status: 'ok', detail: '7 models available', fix: '', ms: 212 },
  { name: 'chat', status: 'ok', detail: "glm-5.3: 'pong'", fix: '', ms: 640 },
  { name: 'structured output', status: 'ok', detail: 'deepseek-v4.1-flash: json_schema · Warsaw, 3 landmarks', fix: '', ms: 1180 },
  { name: 'tool calling', status: 'ok', detail: 'glm-5.3: get_current_time({"city": "Warsaw"})', fix: '', ms: 910 },
  { name: 'embeddings', status: 'ok', detail: 'qwen3-embedding-8b: 1024 dims', fix: '', ms: 150 },
]
