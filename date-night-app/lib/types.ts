export type TransportMode = "walking" | "transit" | "driving";
export type Budget = "$" | "$$" | "$$$" | "$$$$";

export type GenerateRequest = {
  location: string;
  radiusKm: number;
  transportMode: TransportMode;
  vibe: string;
  budget: Budget;
  startTime: string;
  durationMinutes: number;
  partySize: number;
  constraintsNote: string;
};

export type Stop = {
  id: string;
  name: string;
  mapsUrl?: string;
  imageUrl?: string;
};

export type TransportLeg = {
  mode: string;
  durationText: string;
};

export type Plan = {
  id: string;
  title: string;
  vibeLine: string;
  heroImageUrl?: string;
  durationLabel: string;
  costBand: string;
  stops: Stop[];
  transportLegs?: TransportLeg[];
  summary?: string;
};