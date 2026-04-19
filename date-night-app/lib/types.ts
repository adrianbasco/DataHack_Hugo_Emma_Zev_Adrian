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
  transportMode?: TransportMode;
  partySize: number;
  timeWindow?: string;
  desiredIdeaCount: number;
  dietaryConstraints?: string;
  accessibilityConstraints?: string;
  notes?: string;
  selectedTemplateId?: string;
  selectedTemplateTitle?: string;
  selectedTemplateStopTypes?: string[];
  selectedTemplateDurationHours?: number;
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
  selectedTemplateTitle?: string;
  selectedTemplateStopTypes?: string[];
  selectedTemplateDurationHours?: number;
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
  restaurantPhoneNumber?: string;
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

export type RestaurantBookingStatusValue =
  | "queued"
  | "in_progress"
  | "confirmed"
  | "declined"
  | "no_answer"
  | "needs_human_follow_up"
  | "failed"
  | "unknown";

export type BlandCallDescription = {
  provider: "bland_ai";
  phoneNumber: string;
  firstSentence?: string | null;
  task?: string | null;
  voice?: string | null;
  model?: string | null;
  language?: string | null;
  timezone?: string | null;
  maxDurationMinutes?: number | null;
  waitForGreeting: boolean;
  record: boolean;
  voicemail?: Record<string, unknown> | null;
  requestData: Record<string, unknown>;
  metadata: Record<string, unknown>;
  dispositions: string[];
  keywords: string[];
  summaryPrompt?: string | null;
};

export type RestaurantBookingPreview = {
  bookingContext: BookingContext;
  callDescription: BlandCallDescription;
  liveCallEnabled: boolean;
  liveCallDisabledReason?: string | null;
};

export type RestaurantBookingJob = {
  callId: string;
  status: RestaurantBookingStatusValue;
  provider: "bland_ai";
  restaurantName: string;
  restaurantPhoneNumber?: string | null;
  arrivalTimeIso: string;
  partySize: number;
};

export type RestaurantBookingStatus = {
  callId: string;
  status: RestaurantBookingStatusValue;
  providerStatus?: string | null;
  queueStatus?: string | null;
  answeredBy?: string | null;
  summary?: string | null;
  errorMessage?: string | null;
};
