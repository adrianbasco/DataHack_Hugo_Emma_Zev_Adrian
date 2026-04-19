// import { useEffect, useMemo, useState } from "react";
// import { ActivityIndicator, StyleSheet, Text, View } from "react-native";
// import { useRouter } from "expo-router";
// import TemplateCard from "../../components/TemplateCard";
// import {
//   Eyebrow,
//   ScreenShell,
//   SelectChip,
//   SurfaceCard,
//   palette,
// } from "../../components/ui";
// import { fetchTemplates } from "../../lib/api";
// import { DateTemplate } from "../../lib/types";

// const vibeFilters = ["all", "romantic", "foodie", "nightlife", "nerdy", "outdoorsy", "active", "casual"] as const;
// const timeFilters = ["all", "morning", "midday", "afternoon", "evening", "night", "flexible"] as const;

// export default function TemplatesScreen() {
//   const router = useRouter();
//   const [templates, setTemplates] = useState<DateTemplate[]>([]);
//   const [loading, setLoading] = useState(true);
//   const [warning, setWarning] = useState<string | undefined>();
//   const [error, setError] = useState<string | undefined>();
//   const [vibeFilter, setVibeFilter] = useState<(typeof vibeFilters)[number]>("all");
//   const [timeFilter, setTimeFilter] = useState<(typeof timeFilters)[number]>("all");

//   useEffect(() => {
//     let active = true;

//     fetchTemplates()
//       .then((result) => {
//         if (!active) {
//           return;
//         }
//         setTemplates(result.data);
//         setWarning(result.warning);
//       })
//       .catch((fetchError) => {
//         if (!active) {
//           return;
//         }
//         setError(fetchError instanceof Error ? fetchError.message : "Failed to load templates.");
//       })
//       .finally(() => {
//         if (active) {
//           setLoading(false);
//         }
//       });

//     return () => {
//       active = false;
//     };
//   }, []);

//   const filteredTemplates = useMemo(
//     () =>
//       templates.filter((template) => {
//         const vibeMatches =
//           vibeFilter === "all" || template.vibes.includes(vibeFilter);
//         const timeMatches =
//           timeFilter === "all" || template.timeOfDay === timeFilter;
//         return vibeMatches && timeMatches;
//       }),
//     [templates, vibeFilter, timeFilter]
//   );

//   return (
//     <ScreenShell scroll>
//       <View style={styles.hero}>
//         <Eyebrow>Template library</Eyebrow>
//         <Text style={styles.title}>Browse the date arcs your backend can plan against.</Text>
//         <Text style={styles.subtitle}>
//           These templates describe the shape of the date. The backend can later turn them into city-specific plans with real venues.
//         </Text>
//       </View>

//       {warning ? (
//         <SurfaceCard style={styles.infoCard}>
//           <Text style={styles.infoTitle}>Template API note</Text>
//           <Text style={styles.infoText}>{warning}</Text>
//         </SurfaceCard>
//       ) : null}

//       <SurfaceCard style={styles.filtersCard}>
//         <Text style={styles.filterLabel}>Filter by vibe</Text>
//         <View style={styles.filterWrap}>
//           {vibeFilters.map((filter) => (
//             <SelectChip
//               key={filter}
//               label={capitalize(filter)}
//               selected={vibeFilter === filter}
//               onPress={() => setVibeFilter(filter)}
//             />
//           ))}
//         </View>

//         <Text style={styles.filterLabel}>Filter by time of day</Text>
//         <View style={styles.filterWrap}>
//           {timeFilters.map((filter) => (
//             <SelectChip
//               key={filter}
//               label={capitalize(filter)}
//               selected={timeFilter === filter}
//               onPress={() => setTimeFilter(filter)}
//             />
//           ))}
//         </View>
//       </SurfaceCard>

//       {loading ? (
//         <SurfaceCard style={styles.centerCard}>
//           <ActivityIndicator color={palette.accent} />
//           <Text style={styles.centerTitle}>Loading templates</Text>
//           <Text style={styles.centerText}>Pulling the planner arcs into the UI.</Text>
//         </SurfaceCard>
//       ) : null}

//       {!loading && error ? (
//         <SurfaceCard style={styles.centerCard}>
//           <Text style={styles.centerTitle}>Could not load templates</Text>
//           <Text style={styles.centerText}>{error}</Text>
//         </SurfaceCard>
//       ) : null}

//       {!loading && !error
//         ? filteredTemplates.map((template) => (
//             <TemplateCard
//               key={template.id}
//               template={template}
//               onUse={() =>
//                 router.push({
//                   pathname: "/",
//                   params: {
//                     templateId: template.id,
//                     template: JSON.stringify(template),
//                   },
//                 })
//               }
//             />
//           ))
//         : null}

//       {!loading && !error && filteredTemplates.length === 0 ? (
//         <SurfaceCard style={styles.centerCard}>
//           <Text style={styles.centerTitle}>No templates match these filters</Text>
//           <Text style={styles.centerText}>
//             Try a broader vibe or time-of-day selection to bring more date arcs back into view.
//           </Text>
//         </SurfaceCard>
//       ) : null}
//     </ScreenShell>
//   );
// }

// function capitalize(value: string) {
//   return value.charAt(0).toUpperCase() + value.slice(1);
// }

// const styles = StyleSheet.create({
//   hero: {
//     gap: 8,
//   },
//   title: {
//     color: palette.text,
//     fontSize: 34,
//     lineHeight: 40,
//     fontWeight: "900",
//   },
//   subtitle: {
//     color: palette.textMuted,
//     fontSize: 15,
//     lineHeight: 23,
//   },
//   infoCard: {
//     gap: 6,
//   },
//   infoTitle: {
//     color: palette.text,
//     fontSize: 18,
//     fontWeight: "800",
//   },
//   infoText: {
//     color: palette.textMuted,
//     lineHeight: 21,
//   },
//   filtersCard: {
//     gap: 12,
//   },
//   filterLabel: {
//     color: palette.textSoft,
//     fontSize: 14,
//     fontWeight: "700",
//   },
//   filterWrap: {
//     flexDirection: "row",
//     flexWrap: "wrap",
//     gap: 10,
//   },
//   centerCard: {
//     gap: 10,
//     alignItems: "center",
//     paddingVertical: 28,
//   },
//   centerTitle: {
//     color: palette.text,
//     fontSize: 20,
//     fontWeight: "800",
//   },
//   centerText: {
//     color: palette.textMuted,
//     textAlign: "center",
//     lineHeight: 21,
//   },
//   frame: {
//   width: "100%",
//   alignSelf: "center",
//   gap: 18,
//   paddingBottom: 110,
// },
// });
