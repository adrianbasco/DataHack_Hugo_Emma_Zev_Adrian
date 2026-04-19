import React, { useCallback, useMemo, useRef, useState } from "react";
import {
  Animated,
  Image,
  PanResponder,
  Platform,
  Pressable,
  StyleSheet,
  Text,
  View,
  useWindowDimensions,
} from "react-native";

import { Plan } from "../lib/types";
import { ActionButton, SurfaceCard, palette } from "./ui";

const SWIPE_THRESHOLD = 110;
const MAX_CARD_WIDTH = 420;

type Props = {
  plans: Plan[];
  onSavePlan: (plan: Plan) => void | Promise<void>;
  onOpenPlan: (plan: Plan) => void;
  onFinished?: () => void;
};

export default function SwipeDeck({
  plans,
  onSavePlan,
  onOpenPlan,
  onFinished,
}: Props) {
  const { width: screenWidth, height: screenHeight } = useWindowDimensions();

  const [currentIndex, setCurrentIndex] = useState(0);
  const currentIndexRef = useRef(0);
  const position = useRef(new Animated.ValueXY()).current;

  currentIndexRef.current = currentIndex;

  const currentPlan = plans[currentIndex];
  const nextPlan = plans[currentIndex + 1];

  const deckWidth = Math.min(Math.max(screenWidth - 24, 280), MAX_CARD_WIDTH);
  const cardHeight = Math.min(Math.max(screenHeight * 0.68, 500), 700);
  const imageHeight = Math.min(Math.max(cardHeight * 0.5, 220), 320);

  const rotate = position.x.interpolate({
    inputRange: [-deckWidth, 0, deckWidth],
    outputRange: ["-14deg", "0deg", "14deg"],
    extrapolate: "clamp",
  });

  const likeOpacity = position.x.interpolate({
    inputRange: [0, 80, 160],
    outputRange: [0, 0.6, 1],
    extrapolate: "clamp",
  });

  const nopeOpacity = position.x.interpolate({
    inputRange: [-160, -80, 0],
    outputRange: [1, 0.6, 0],
    extrapolate: "clamp",
  });

  const resetPosition = useCallback(() => {
    Animated.spring(position, {
      toValue: { x: 0, y: 0 },
      useNativeDriver: false,
      friction: 5,
      tension: 90,
    }).start();
  }, [position]);

  const goToNext = useCallback(
  async (direction: "left" | "right") => {
    const idx = currentIndexRef.current;
    const swipedPlan = plans[idx];

    if (direction === "right" && swipedPlan) {
      try {
        await onSavePlan(swipedPlan);
      } catch (err) {
        console.error("Failed to save swiped plan.", err);
      }
    }

    position.setValue({ x: 0, y: 0 });

    const next = idx + 1;
    currentIndexRef.current = next;
    setCurrentIndex(next);

    if (next >= plans.length) {
      onFinished?.();
    }
  },
  [onFinished, onSavePlan, plans, position]
);

  const forceSwipe = useCallback(
  (direction: "left" | "right") => {
    const x = direction === "right" ? deckWidth + 140 : -deckWidth - 140;

    Animated.timing(position, {
      toValue: { x, y: 0 },
      duration: 220,
      useNativeDriver: false,
    }).start(() => {
      void goToNext(direction);
    });
  },
  [deckWidth, goToNext, position]
);

const panResponder = useMemo(
  () =>
    PanResponder.create({
      onStartShouldSetPanResponder: () => false,
      onMoveShouldSetPanResponder: (_, gestureState) =>
        Math.abs(gestureState.dx) > Math.abs(gestureState.dy) &&
        Math.abs(gestureState.dx) > 6,
      onMoveShouldSetPanResponderCapture: (_, gestureState) =>
        Math.abs(gestureState.dx) > Math.abs(gestureState.dy) &&
        Math.abs(gestureState.dx) > 6,
      onPanResponderMove: (_, gestureState) => {
        position.setValue({
          x: gestureState.dx,
          y: gestureState.dy * 0.18,
        });
      },
      onPanResponderRelease: (_, gestureState) => {
        if (gestureState.dx > SWIPE_THRESHOLD) {
          forceSwipe("right");
        } else if (gestureState.dx < -SWIPE_THRESHOLD) {
          forceSwipe("left");
        } else {
          resetPosition();
        }
      },
      onPanResponderTerminate: resetPosition,
      onPanResponderTerminationRequest: () => false,
    }),
  [forceSwipe, position, resetPosition]
);
  if (!currentPlan) {
    return (
      <View style={styles.flex}>
        <SurfaceCard style={styles.emptyWrap}>
          <Text style={styles.emptyTitle}>All done!</Text>
          <Text style={styles.emptyText}>
            Open saved dates to revisit your picks.
          </Text>
        </SurfaceCard>
      </View>
    );
  }

  return (
    <View style={styles.flex}>
      <View style={styles.container}>
        <View style={[styles.deckArea, { width: deckWidth, minHeight: cardHeight }]}>
          {nextPlan ? (
            <View
              style={[
                styles.card,
                styles.nextCard,
                { width: deckWidth, minHeight: cardHeight },
              ]}
              pointerEvents="none"
            >
              {nextPlan.heroImageUrl ? (
                <Image
                  source={{ uri: nextPlan.heroImageUrl }}
                  style={[styles.image, { height: imageHeight }]}
                />
              ) : (
                <View
                  style={[
                    styles.image,
                    styles.imagePlaceholder,
                    { height: imageHeight },
                  ]}
                />
              )}

              <View style={styles.cardBody}>
                <Text style={styles.kicker}>Up next</Text>
                <Text style={styles.title} numberOfLines={1}>
                  {nextPlan.title}
                </Text>
              </View>
            </View>
          ) : null}

          <Animated.View
            {...panResponder.panHandlers}
            style={[
              styles.card,
              {
                width: deckWidth,
                minHeight: cardHeight,
                transform: [...position.getTranslateTransform(), { rotate }],
              },
            ]}
          >
            <Pressable
              style={styles.cardPressable}
              onPress={() => onOpenPlan(currentPlan)}
            >
              {currentPlan.heroImageUrl ? (
                <Image
                  source={{ uri: currentPlan.heroImageUrl }}
                  style={[styles.image, { height: imageHeight }]}
                />
              ) : (
                <View
                  style={[
                    styles.image,
                    styles.imagePlaceholder,
                    { height: imageHeight },
                  ]}
                />
              )}

              <Animated.View style={[styles.likeBadge, { opacity: likeOpacity }]}>
                <Text style={styles.likeBadgeText}>SAVE</Text>
              </Animated.View>

              <Animated.View style={[styles.nopeBadge, { opacity: nopeOpacity }]}>
                <Text style={styles.nopeBadgeText}>SKIP</Text>
              </Animated.View>

              <View style={styles.cardBody}>
                <Text style={styles.kicker}>
                  {currentPlan.templateHint || "Planner suggestion"}
                </Text>

                <Text style={styles.title}>{currentPlan.title}</Text>

                <Text style={styles.subtitle} numberOfLines={2}>
                  {currentPlan.hook}
                </Text>

                <View style={styles.metaRow}>
                  {currentPlan.durationLabel ? (
                    <MetaPill label={currentPlan.durationLabel} />
                  ) : null}
                  {currentPlan.costBand ? (
                    <MetaPill label={currentPlan.costBand} />
                  ) : null}
                  {currentPlan.weather ? (
                    <MetaPill label={currentPlan.weather} tone="cool" />
                  ) : null}
                </View>

                <View style={styles.stopList}>
                  {currentPlan.stops.slice(0, 3).map((stop, index) => (
                    <View key={stop.id} style={styles.stopRow}>
                      <Text style={styles.stopIndex}>{index + 1}</Text>
                      <Text style={styles.stopText} numberOfLines={1}>
                        {stop.name}
                      </Text>
                    </View>
                  ))}
                </View>

                <Text style={styles.tapHint}>Tap for full details & booking</Text>
              </View>
            </Pressable>
          </Animated.View>
        </View>

        <View style={[styles.actions, { width: deckWidth }]}>
          <ActionButton
            label="Skip"
            variant="secondary"
            onPress={() => forceSwipe("left")}
            style={[styles.actionButton, styles.actionButtonLeft]}
          />
          <ActionButton
            label="Save plan ♡"
            onPress={() => forceSwipe("right")}
            style={styles.actionButton}
          />
        </View>
      </View>
    </View>
  );
}

function MetaPill({
  label,
  tone = "default",
}: {
  label: string;
  tone?: "default" | "cool";
}) {
  return (
    <View style={[styles.metaPill, tone === "cool" && styles.metaPillCool]}>
      <Text
        style={[styles.metaPillText, tone === "cool" && styles.metaPillTextCool]}
      >
        {label}
      </Text>
    </View>
  );
}

const styles = StyleSheet.create({
  flex: {
    flex: 1,
  },
  container: {
    flex: 1,
    alignItems: "center",
    justifyContent: "center",
    paddingHorizontal: 12,
    paddingTop: 8,
    paddingBottom: 12,
  },
  deckArea: {
    flex: 1,
    width: "100%",
    alignItems: "center",
    justifyContent: "center",
    position: "relative",
  },
  card: {
    position: "absolute",
    width: "100%",
    backgroundColor: palette.panel,
    borderRadius: 28,
    overflow: "hidden",
    borderWidth: 1,
    borderColor: palette.border,
    shadowColor: "#020617",
    shadowOpacity: 0.3,
    shadowRadius: 24,
    shadowOffset: { width: 0, height: 14 },
    elevation: 10,
  },
  nextCard: {
    transform: [{ scale: 0.96 }, { translateY: 12 }],
    opacity: 0.45,
  },
  cardPressable: {
    flex: 1,
  },
  image: {
    width: "100%",
  },
  imagePlaceholder: {
    backgroundColor: "rgba(18, 36, 58, 0.9)",
  },
  cardBody: {
    paddingHorizontal: 18,
    paddingTop: 18,
    paddingBottom: 18,
  },
  kicker: {
    color: palette.accentWarm,
    fontSize: 11,
    fontWeight: "800",
    letterSpacing: 0.8,
    textTransform: "uppercase",
    marginBottom: 8,
  },
  title: {
    fontSize: 24,
    lineHeight: 29,
    fontWeight: "900",
    color: palette.text,
    marginBottom: 8,
  },
  subtitle: {
    fontSize: 14,
    lineHeight: 21,
    color: palette.textMuted,
    marginBottom: 12,
  },
  metaRow: {
    flexDirection: "row",
    flexWrap: "wrap",
    marginBottom: 10,
  },
  metaPill: {
    borderRadius: 999,
    paddingHorizontal: 10,
    paddingVertical: 6,
    backgroundColor: "rgba(255, 255, 255, 0.06)",
    borderWidth: 1,
    borderColor: palette.border,
    marginRight: 8,
    marginBottom: 8,
  },
  metaPillCool: {
    backgroundColor: "rgba(52, 211, 153, 0.12)",
    borderColor: "rgba(52, 211, 153, 0.24)",
  },
  metaPillText: {
    fontSize: 11,
    fontWeight: "700",
    color: palette.textSoft,
  },
  metaPillTextCool: {
    color: "#b4f5dd",
  },
  stopList: {
    marginBottom: 10,
  },
  stopRow: {
    flexDirection: "row",
    alignItems: "center",
    marginBottom: 8,
  },
  stopIndex: {
    width: 22,
    color: palette.accent,
    fontWeight: "900",
    fontSize: 13,
  },
  stopText: {
    color: palette.textSoft,
    fontSize: 13,
    fontWeight: "600",
    flex: 1,
  },
  tapHint: {
    color: palette.accentWarm,
    fontSize: 12,
    fontWeight: "700",
    opacity: 0.7,
  },
  likeBadge: {
    position: "absolute",
    top: 20,
    right: 16,
    borderWidth: 2,
    borderColor: palette.success,
    paddingHorizontal: 12,
    paddingVertical: 6,
    borderRadius: 10,
    transform: [{ rotate: "10deg" }],
    backgroundColor: "rgba(7, 17, 31, 0.84)",
    zIndex: 2,
  },
  likeBadgeText: {
    color: "#a7f3d0",
    fontWeight: "900",
    fontSize: 16,
  },
  nopeBadge: {
    position: "absolute",
    top: 20,
    left: 16,
    borderWidth: 2,
    borderColor: palette.danger,
    paddingHorizontal: 12,
    paddingVertical: 6,
    borderRadius: 10,
    transform: [{ rotate: "-10deg" }],
    backgroundColor: "rgba(7, 17, 31, 0.84)",
    zIndex: 2,
  },
  nopeBadgeText: {
    color: "#fecdd3",
    fontWeight: "900",
    fontSize: 16,
  },
  actions: {
    flexDirection: "row",
    alignItems: "center",
    marginTop: 12,
  },
  actionButton: {
    flex: 1,
  },
  actionButtonLeft: {
    marginRight: 12,
  },
  emptyWrap: {
    flex: 1,
    alignItems: "center",
    justifyContent: "center",
    paddingVertical: 36,
    paddingHorizontal: 24,
  },
  emptyTitle: {
    fontSize: 24,
    fontWeight: "900",
    color: palette.text,
    marginBottom: 10,
  },
  emptyText: {
    color: palette.textMuted,
    fontSize: 15,
    lineHeight: 22,
    textAlign: "center",
  },
});