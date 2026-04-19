import { Plan } from "./types";

export const mockPlans: Plan[] = [
  {
    id: "1",
    title: "Romantic City Lights",
    vibeLine: "Elegant & intimate evening under the stars",
    heroImageUrl:
      "https://images.unsplash.com/photo-1519167758481-83f550bb49b3?auto=format&fit=crop&w=1200&q=80",
    durationLabel: "4 hours",
    costBand: "$$$",
    weather: "Clear",
    summary:
      "Start with rooftop cocktails, settle into a candlelit dinner, then finish with a waterfront stroll.",
    stops: [
      {
        id: "1-1",
        name: "Sunset Rooftop Bar",
        description: "Cocktails with skyline views",
        time: "6:00 PM",
      },
      {
        id: "1-2",
        name: "La Petite Maison",
        description: "French dinner in a warm, intimate setting",
        time: "7:30 PM",
        transport: "5 min walk",
      },
      {
        id: "1-3",
        name: "Waterfront Stroll",
        description: "A relaxed walk by the water",
        time: "9:30 PM",
        transport: "10 min drive",
      },
    ],
    transportLegs: [
      { mode: "Walk", durationText: "5 min" },
      { mode: "Drive", durationText: "10 min" },
    ],
  },
  {
    id: "2",
    title: "Cozy Wine & Dine",
    vibeLine: "Warm, intimate, and conversation-focused",
    heroImageUrl:
      "https://images.unsplash.com/photo-1514933651103-005eec06c04b?auto=format&fit=crop&w=1200&q=80",
    durationLabel: "3.5 hours",
    costBand: "$$",
    weather: "Partly cloudy",
    summary:
      "A slower, softer night built around wine, a comforting meal, and a low-key final stop.",
    stops: [
      {
        id: "2-1",
        name: "Wine Tasting Room",
        description: "Local wines and shared plates",
        time: "5:30 PM",
      },
      {
        id: "2-2",
        name: "Trattoria Bella",
        description: "Rustic Italian dinner",
        time: "7:00 PM",
        transport: "3 min walk",
      },
      {
        id: "2-3",
        name: "Jazz Lounge",
        description: "Nightcaps and live jazz",
        time: "9:00 PM",
        transport: "7 min drive",
      },
    ],
    transportLegs: [
      { mode: "Walk", durationText: "3 min" },
      { mode: "Drive", durationText: "7 min" },
    ],
  },
  {
    id: "3",
    title: "Culinary Adventure",
    vibeLine: "Playful, delicious, and Instagram-worthy",
    heroImageUrl:
      "https://images.unsplash.com/photo-1414235077428-338989a2e8c0?auto=format&fit=crop&w=1200&q=80",
    durationLabel: "5 hours",
    costBand: "$$$",
    weather: "Sunny",
    summary:
      "An energetic date with multiple stops, dessert, and a stylish finish with city views.",
    stops: [
      {
        id: "3-1",
        name: "Street Food Market",
        description: "Start with shared bites and casual energy",
        time: "4:00 PM",
      },
      {
        id: "3-2",
        name: "Dessert Laboratory",
        description: "Interactive dessert stop",
        time: "6:00 PM",
        transport: "12 min transit",
      },
      {
        id: "3-3",
        name: "Skyline Lounge",
        description: "Cocktails with a view",
        time: "8:30 PM",
        transport: "5 min walk",
      },
    ],
    transportLegs: [
      { mode: "Transit", durationText: "12 min" },
      { mode: "Walk", durationText: "5 min" },
    ],
  },
  {
    id: "4",
    title: "Sunset & Sips",
    vibeLine: "Laid-back, scenic, and effortlessly cool",
    heroImageUrl:
      "https://images.unsplash.com/photo-1507525428034-b723cf961d3e?auto=format&fit=crop&w=1200&q=80",
    durationLabel: "3 hours",
    costBand: "$$",
    weather: "Clear skies",
    summary:
      "A lighter date with views, dinner, and a relaxed final stop that feels easy and natural.",
    stops: [
      {
        id: "4-1",
        name: "Beachside Café",
        description: "Coffee and pastries by the water",
        time: "5:00 PM",
      },
      {
        id: "4-2",
        name: "Coastal Grill",
        description: "Seafood dinner at sunset",
        time: "6:30 PM",
        transport: "2 min walk",
      },
      {
        id: "4-3",
        name: "Bonfire Lounge",
        description: "Drinks and dessert by the fire",
        time: "8:30 PM",
        transport: "5 min walk",
      },
    ],
    transportLegs: [
      { mode: "Walk", durationText: "2 min" },
      { mode: "Walk", durationText: "5 min" },
    ],
  },
];