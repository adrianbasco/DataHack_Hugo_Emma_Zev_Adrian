import { Plan } from "./types";

export const mockPlans: Plan[] = [
  {
    id: "1",
    title: "Harbour Sunset Date",
    vibeLine: "Relaxed waterfront dinner and dessert",
    heroImageUrl: "https://images.unsplash.com/photo-1507525428034-b723cf961d3e",
    durationLabel: "3 hours",
    costBand: "$$",
    summary: "Start with a walk, then dinner, then dessert by the water.",
    stops: [
      { id: "s1", name: "Barangaroo Reserve" },
      { id: "s2", name: "Darling Harbour Dinner Spot" },
      { id: "s3", name: "Gelato Stop" },
    ],
    transportLegs: [
      { mode: "walk", durationText: "12 min" },
      { mode: "walk", durationText: "8 min" },
    ],
  },
  {
    id: "2",
    title: "Nerdy Museum Night",
    vibeLine: "Exhibits, coffee, and a late dessert stop",
    durationLabel: "2.5 hours",
    costBand: "$$",
    summary: "A playful date with something to talk about the whole night.",
    stops: [
      { id: "s4", name: "Museum Visit" },
      { id: "s5", name: "Cafe" },
      { id: "s6", name: "Dessert Bar" },
    ],
    transportLegs: [
      { mode: "walk", durationText: "10 min" },
      { mode: "walk", durationText: "6 min" },
    ],
  },
];