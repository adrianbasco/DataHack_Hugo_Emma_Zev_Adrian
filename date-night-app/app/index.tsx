// import { useMemo } from "react";
// import { StyleSheet, Text, View } from "react-native";
// import { useLocalSearchParams, useRouter } from "expo-router";
// import {
//   Eyebrow,
//   ScreenShell,
//   SelectChip,
//   SurfaceCard,
//   palette,
// } from "../components/ui";
// import {
//   DateTemplate,
//   GenerateChatRequest,
// } from "../lib/types";

// export default function HomeScreen() {
//   const router = useRouter();
//   const params = useLocalSearchParams<{ templateId?: string; template?: string }>();

//   const selectedTemplate = useMemo(() => {
//     if (typeof params.template === "string" && params.template) {
//       try {
//         return JSON.parse(params.template) as DateTemplate;
//       } catch (error) {
//         console.error("Template route param could not be parsed.", error);
//         return undefined;
//       }
//     }
//     return undefined;
//   }, [params.template, params.templateId]);

//   function handleChatSubmit(payload: GenerateChatRequest) {
//     router.push({
//       pathname: "/results",
//       params: { request: JSON.stringify(payload) },
//     });
//   }

//   return (
//     <ScreenShell scroll contentContainerStyle={styles.container}>
//       {/* ── Hero ── */}
//       <View style={styles.heroSection}>
//         <Eyebrow tone="warm">Date Night</Eyebrow>
//         <Text style={styles.heroTitle}>Plan your perfect date.</Text>
//         <Text style={styles.heroSubtitle}>
//           Tell us the vibe, location, and budget — we'll build the itinerary.
//         </Text>
//       </View>

//       {/* ── Selected template badge (only when one is active) ── */}
//       {selectedTemplate ? (
//         <SurfaceCard style={styles.templateBadge}>
//           <Text style={styles.templateBadgeLabel}>Template selected</Text>
//           <Text style={styles.templateBadgeTitle}>{selectedTemplate.title}</Text>
//           <View style={styles.templateMeta}>
//             <SelectChip label={selectedTemplate.timeOfDay} selected />
//             <SelectChip label={`${selectedTemplate.durationHours}h`} selected />
//             {selectedTemplate.vibes.map((vibe) => (
//               <SelectChip key={vibe} label={vibe} selected />
//             ))}
//           </View>
//         </SurfaceCard>
//       ) : null}
//     </ScreenShell>
//   );
// }

// const styles = StyleSheet.create({
//   container: {
//     gap: 16,
//   },
//   heroSection: {
//     gap: 8,
//     paddingTop: 4,
//   },
//   heroTitle: {
//     color: palette.text,
//     fontSize: 36,
//     lineHeight: 42,
//     fontWeight: "900",
//   },
//   heroSubtitle: {
//     color: palette.textMuted,
//     fontSize: 15,
//     lineHeight: 23,
//   },
//   // Template badge is compact so it does not compete with the planner.
//   templateBadge: {
//     gap: 6,
//     paddingVertical: 14,
//   },
//   templateBadgeLabel: {
//     color: palette.accentWarm,
//     fontSize: 11,
//     fontWeight: "800",
//     letterSpacing: 0.8,
//     textTransform: "uppercase",
//   },
//   templateBadgeTitle: {
//     color: palette.text,
//     fontSize: 18,
//     fontWeight: "800",
//   },
//   templateMeta: {
//     flexDirection: "row",
//     flexWrap: "wrap",
//     gap: 8,
//     marginTop: 4,
//   },
// });
