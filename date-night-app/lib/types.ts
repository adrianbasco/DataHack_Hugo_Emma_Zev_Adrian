export type TransportMode = "walking" | "public_transport" | "driving";
export type Budget = "$" | "$$" | "$$$" | "$$$$";
export type PlannerMode = "form" | "chat";
export type Vibe =
  | "romantic"
  | "foodie"
  | "nightlife"
  | "nerdy"
  | "outdoorsy"
  | "active"
  | "casual";
export type TimeOfDay =
  | "morning"
  | "midday"
  | "afternoon"
  | "evening"
  | "night"
  | "flexible";

export type PlannerMessage = {
  id: string;
  role: "assistant" | "user";
  content: string;
};

export type GenerateFormRequest = {
  location: string;
  vibes: Vibe[];
  radiusKm: number;
  budget?: Budget | "";
  transportMode: TransportMode;
  partySize: number;
  timeWindow?: string;
  desiredIdeaCount: number;
  dietaryConstraints?: string;
  accessibilityConstraints?: string;
  notes?: string;
  selectedTemplateId?: string;
};

export type GenerateChatRequest = {
  prompt: string;
  transcript: PlannerMessage[];
  location?: string;
  timeWindow?: string;
  vibe?: string;
  budget?: Budget | "";
  transportMode?: TransportMode;
  partySize: number;
  constraints?: string;
  desiredIdeaCount: number;
  selectedTemplateId?: string;
};

export type TemplateStop = {
  type: string;
  kind?: "connective" | "venue";
  note?: string;
};

export type DateTemplate = {
  id: string;
  title: string;
  vibes: Vibe[];
  timeOfDay: TimeOfDay;
  durationHours: number;
  meaningfulVariations: number;
  weatherSensitive: boolean;
  description: string;
  stops: TemplateStop[];
};

export type PlanStop = {
  id: string;
  kind: "connective" | "venue";
  stopType: string;
  name: string;
  description: string;
  whyItFits?: string;
  fsqPlaceId?: string | null;
  time?: string;
  transport?: string;
  mapsUrl?: string;
  address?: string;
  phoneNumber?: string;
};

export type TransportLeg = {
  mode: string;
  durationText: string;
};

export type BookingContext = {
  planId?: string;
  restaurantName?: string;
  restaurantPhoneNumber?: string;
  restaurantAddress?: string;
  suggestedArrivalTimeIso?: string;
  partySize?: number;
};

export type Plan = {
  id: string;
  title: string;
  hook: string;
  summary?: string;
  vibes: string[];
  templateHint?: string | null;
  templateId?: string | null;
  durationLabel?: string;
  costBand?: string;
  weather?: string;
  heroImageUrl?: string;
  mapsVerificationNeeded?: boolean;
  constraintsConsidered: string[];
  stops: PlanStop[];
  transportLegs?: TransportLeg[];
  bookingContext?: BookingContext;
  source: "api" | "fallback";
};

export type DataResult<T> = {
  data: T;
  source: "api" | "fallback";
  warning?: string;
};

export type RestaurantBookingRequest = {
  restaurantName: string;
  restaurantPhoneNumber: string;
  arrivalTimeIso: string;
  partySize: number;
  bookingName: string;
  customerPhoneNumber?: string;
  restaurantAddress?: string;
  dietaryConstraints?: string;
  accessibilityConstraints?: string;
  specialOccasion?: string;
  notes?: string;
  acceptableTimeWindowMinutes?: number;
  planId?: string;
};

export type RestaurantBookingJob = {
  callId: string;
  status: string;
  provider: string;
  restaurantName: string;
  restaurantPhoneNumber: string;
  arrivalTimeIso: string;
  partySize: number;
};

export type RestaurantBookingStatus = {
  callId: string;
  status: string;
  providerStatus?: string | null;
  queueStatus?: string | null;
  answeredBy?: string | null;
  summary?: string | null;
  errorMessage?: string | null;
};
