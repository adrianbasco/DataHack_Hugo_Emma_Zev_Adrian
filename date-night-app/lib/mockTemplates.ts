import { DateTemplate } from "./types";

export const fallbackTemplates: DateTemplate[] = [
  {
    id: "coffee_and_stroll",
    title: "Coffee and a stroll",
    vibes: ["casual", "romantic"],
    timeOfDay: "morning",
    durationHours: 1.5,
    meaningfulVariations: 18,
    weatherSensitive: true,
    description:
      "Slow start with good coffee, then an easy walk somewhere green or along the water.",
    stops: [
      { type: "cafe", kind: "venue" },
      {
        type: "park_or_garden",
        kind: "connective",
        note: "Leisurely walk, no fixed venue.",
      },
    ],
  },
  {
    id: "brunch_and_bookstore",
    title: "Brunch and a bookstore",
    vibes: ["casual", "nerdy", "foodie"],
    timeOfDay: "morning",
    durationHours: 2.5,
    meaningfulVariations: 10,
    weatherSensitive: false,
    description:
      "Long brunch, then browse a bookstore together and pick something for each other.",
    stops: [
      { type: "brunch_restaurant", kind: "venue" },
      { type: "bookstore", kind: "venue" },
    ],
  },
  {
    id: "gallery_and_wine_bar",
    title: "Gallery and wine bar",
    vibes: ["romantic", "nerdy"],
    timeOfDay: "afternoon",
    durationHours: 2.5,
    meaningfulVariations: 9,
    weatherSensitive: false,
    description:
      "Drift through an art gallery, then unwind over a glass of wine nearby.",
    stops: [
      { type: "art_gallery", kind: "venue" },
      { type: "wine_bar", kind: "venue" },
    ],
  },
  {
    id: "sunset_lookout_and_dinner",
    title: "Sunset lookout, then dinner",
    vibes: ["romantic", "outdoorsy"],
    timeOfDay: "evening",
    durationHours: 3.5,
    meaningfulVariations: 16,
    weatherSensitive: true,
    description:
      "Catch the sunset from a scenic lookout, then move into a proper dinner after dark.",
    stops: [
      {
        type: "scenic_lookout",
        kind: "connective",
        note: "Sunset from a scenic lookout.",
      },
      { type: "restaurant", kind: "venue" },
    ],
  },
  {
    id: "drinks_dinner_dessert",
    title: "Drinks, dinner, dessert",
    vibes: ["romantic", "foodie"],
    timeOfDay: "evening",
    durationHours: 4,
    meaningfulVariations: 30,
    weatherSensitive: false,
    description:
      "The classic arc: pre-dinner drinks, a proper dinner, and somewhere sweet to finish.",
    stops: [
      { type: "cocktail_bar", kind: "venue" },
      { type: "restaurant", kind: "venue" },
      { type: "dessert_shop", kind: "venue" },
    ],
  },
  {
    id: "pre_theatre_dinner_and_show",
    title: "Pre-theatre dinner and a show",
    vibes: ["romantic", "nerdy"],
    timeOfDay: "evening",
    durationHours: 4,
    meaningfulVariations: 14,
    weatherSensitive: false,
    description:
      "Early dinner, then a performance, theatre, ballet, symphony, or whatever is on.",
    stops: [
      { type: "restaurant", kind: "venue" },
      { type: "performing_arts_venue", kind: "venue" },
    ],
  },
  {
    id: "dinner_and_a_movie",
    title: "Dinner and a movie",
    vibes: ["casual", "romantic"],
    timeOfDay: "evening",
    durationHours: 4,
    meaningfulVariations: 18,
    weatherSensitive: false,
    description: "Casual dinner, then a cinema. Old-school for a reason.",
    stops: [
      { type: "casual_restaurant", kind: "venue" },
      { type: "movie_theater", kind: "venue" },
    ],
  },
  {
    id: "live_music_and_late_drinks",
    title: "Live music and late drinks",
    vibes: ["nightlife", "romantic"],
    timeOfDay: "night",
    durationHours: 4,
    meaningfulVariations: 14,
    weatherSensitive: false,
    description:
      "A proper dinner, a live music set, then a last drink somewhere quieter.",
    stops: [
      { type: "restaurant", kind: "venue" },
      { type: "live_music_venue", kind: "venue" },
      { type: "bar", kind: "venue" },
    ],
  },
  {
    id: "mini_golf_food_bar",
    title: "Mini golf, casual food, a drink",
    vibes: ["active", "casual", "nightlife"],
    timeOfDay: "evening",
    durationHours: 3,
    meaningfulVariations: 3,
    weatherSensitive: false,
    description:
      "Low-stakes competitive energy with mini golf, casual food, and one drink after.",
    stops: [
      { type: "mini_golf", kind: "venue" },
      { type: "casual_restaurant", kind: "venue" },
      { type: "bar", kind: "venue" },
    ],
  },
  {
    id: "escape_room_and_dinner",
    title: "Escape room and dinner",
    vibes: ["active", "nerdy", "casual"],
    timeOfDay: "evening",
    durationHours: 3.5,
    meaningfulVariations: 10,
    weatherSensitive: false,
    description:
      "Solve something together, then talk about it over dinner and a drink.",
    stops: [
      { type: "escape_room", kind: "venue" },
      { type: "restaurant", kind: "venue" },
      { type: "bar", kind: "venue" },
    ],
  },
  {
    id: "beach_day_into_seafood",
    title: "Beach day into seafood dinner",
    vibes: ["outdoorsy", "romantic", "foodie"],
    timeOfDay: "afternoon",
    durationHours: 5,
    meaningfulVariations: 12,
    weatherSensitive: true,
    description:
      "Beach in the afternoon, clean up, then seafood dinner as the sun goes down.",
    stops: [
      { type: "beach", kind: "venue" },
      { type: "seafood_restaurant", kind: "venue" },
    ],
  },
  {
    id: "rainy_day_indoors",
    title: "Rainy day indoors",
    vibes: ["casual", "nerdy", "romantic"],
    timeOfDay: "flexible",
    durationHours: 4,
    meaningfulVariations: 12,
    weatherSensitive: false,
    description:
      "Whole afternoon inside with bookstore, coffee, museum, and dinner to end.",
    stops: [
      { type: "bookstore", kind: "venue" },
      { type: "cafe", kind: "venue" },
      { type: "museum", kind: "venue" },
      { type: "restaurant", kind: "venue" },
    ],
  },
  {
    id: "dessert_crawl",
    title: "Dessert crawl",
    vibes: ["foodie", "casual", "romantic"],
    timeOfDay: "afternoon",
    durationHours: 2,
    meaningfulVariations: 16,
    weatherSensitive: false,
    description:
      "Three small sweet stops, a pastry, an ice cream, and something chocolate.",
    stops: [
      { type: "bakery", kind: "venue" },
      { type: "dessert_shop", kind: "venue" },
      { type: "dessert_shop", kind: "venue" },
    ],
  },
];

export function findFallbackTemplateById(id?: string | null) {
  if (!id) {
    return undefined;
  }
  return fallbackTemplates.find((template) => template.id === id);
}
