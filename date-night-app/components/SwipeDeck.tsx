import React, { useMemo, useRef, useState } from "react";
import {
  Animated,
  Dimensions,
  Image,
  PanResponder,
  Pressable,
  StyleSheet,
  Text,
  View,
} from "react-native";
import { Plan } from "../lib/types";
import { ActionButton, SurfaceCard, palette } from "./ui";

const SCREEN_WIDTH = Dimensions.get("window").width;
const SWIPE_THRESHOLD = 120;

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
  const [currentIndex, setCurrentIndex] = useState(0);
  const position = useRef(new Animated.ValueXY()).current;

  const currentPlan = plans[currentIndex];
  const nextPlan = plans[currentIndex + 1];

  const rotate = position.x.interpolate({
    inputRange: [-SCREEN_WIDTH, 0, SCREEN_WIDTH],
    outputRange: ["-16deg", "0deg", "16deg"],
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

  const animatedCardStyle = useMemo(
    () => ({
      transform: [...position.getTranslateTransform(), { rotate }],
    }),
    [position, rotate]
  );

  function resetPosition() {
    Animated.spring(position, {
      toValue: { x: 0, y: 0 },
      useNativeDriver: false,
      friction: 5,
    }).start();
  }

  function goToNext(direction: "left" | "right") {
    const swipedPlan = plans[currentIndex];

    if (direction === "right" && swipedPlan) {
      onSavePlan(swipedPlan);
    }

    position.setValue({ x: 0, y: 0 });

    const nextIndex = currentIndex + 1;
    if (nextIndex >= plans.length) {
      setCurrentIndex(nextIndex);
      onFinished?.();
      return;
    }

    setCurrentIndex(nextIndex);
  }

  function forceSwipe(direction: "left" | "right") {
    const x = direction === "right" ? SCREEN_WIDTH + 120 : -SCREEN_WIDTH - 120;

    Animated.timing(position, {
      toValue: { x, y: 0 },
      duration: 220,
      useNativeDriver: false,
    }).start(() => {
      goToNext(direction);
    });
  }

  const panResponder = useRef(
    PanResponder.create({
      onMoveShouldSetPanResponder: (_evt, gestureState) =>
        Math.abs(gestureState.dx) > 8 || Math.abs(gestureState.dy) > 8,
      onPanResponderMove: (_evt, gestureState) => {
        position.setValue({ x: gestureState.dx, y: gestureState.dy * 0.18 });
      },
      onPanResponderRelease: (_evt, gestureState) => {
        if (gestureState.dx > SWIPE_THRESHOLD) {
          forceSwipe("right");
        } else if (gestureState.dx < -SWIPE_THRESHOLD) {
          forceSwipe("left");
        } else {
          resetPosition();
        }
      },
    })
  ).current;

  if (!currentPlan) {
    return (
      <SurfaceCard style={styles.emptyWrap}>
        <Text style={styles.emptyTitle}>Deck complete</Text>
        <Text style={styles.emptyText}>
          You reached the end. Head to saved plans to revisit your favorites.
        </Text>
      </SurfaceCard>
    );
  }

  return (
    <View style={styles.container}>
      <View style={styles.deckArea}>
        {nextPlan ? (
          <View style={[styles.card, styles.nextCard]}>
            {nextPlan.heroImageUrl ? (
              <Image source={{ uri: nextPlan.heroImageUrl }} style={styles.image} />
            ) : null}
            <View style={styles.cardBody}>
              <Text style={styles.kicker}>Up next</Text>
              <Text style={styles.title}>{nextPlan.title}</Text>
              <Text style={styles.subtitle}>{nextPlan.vibeLine}</Text>
            </View>
          </View>
        ) : null}

        <Animated.View
          style={[styles.card, animatedCardStyle]}
          {...panResponder.panHandlers}
        >
          <Pressable style={styles.cardPressable} onPress={() => onOpenPlan(currentPlan)}>
            {currentPlan.heroImageUrl ? (
              <Image source={{ uri: currentPlan.heroImageUrl }} style={styles.image} />
            ) : null}

            <Animated.View style={[styles.likeBadge, { opacity: likeOpacity }]}>
              <Text style={styles.likeBadgeText}>SAVE</Text>
            </Animated.View>

            <Animated.View style={[styles.nopeBadge, { opacity: nopeOpacity }]}>
              <Text style={styles.nopeBadgeText}>SKIP</Text>
            </Animated.View>

            <View style={styles.cardBody}>
              <Text style={styles.kicker}>Tonight's pick</Text>
              <Text style={styles.title}>{currentPlan.title}</Text>
              <Text style={styles.subtitle}>{currentPlan.vibeLine}</Text>

              <View style={styles.metaRow}>
                <MetaPill label={currentPlan.durationLabel} />
                <MetaPill label={currentPlan.costBand} />
                {currentPlan.weather ? (
                  <MetaPill label={currentPlan.weather} tone="cool" />
                ) : null}
              </View>

              <View style={styles.stopList}>
                {currentPlan.stops.slice(0, 3).map((stop, index) => (
                  <View key={stop.id} style={styles.stopRow}>
                    <Text style={styles.stopIndex}>{index + 1}</Text>
                    <Text style={styles.stopText}>{stop.name}</Text>
                  </View>
                ))}
              </View>

              <Text style={styles.tapHint}>Tap the card for the full itinerary</Text>
            </View>
          </Pressable>
        </Animated.View>
      </View>

      <View style={styles.actions}>
        <ActionButton
          label="Skip"
          variant="secondary"
          onPress={() => forceSwipe("left")}
          style={styles.actionButton}
        />
        <ActionButton
          label="Save plan"
          onPress={() => forceSwipe("right")}
          style={styles.actionButton}
        />
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
      <Text style={[styles.metaPillText, tone === "cool" && styles.metaPillTextCool]}>
        {label}
      </Text>
    </View>
  );
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
    gap: 16,
  },
  deckArea: {
    flex: 1,
    justifyContent: "center",
    alignItems: "center",
    minHeight: 580,
  },
  card: {
    width: "100%",
    maxWidth: 420,
    backgroundColor: palette.panel,
    borderRadius: 30,
    overflow: "hidden",
    position: "absolute",
    borderWidth: 1,
    borderColor: palette.border,
    shadowColor: "#020617",
    shadowOpacity: 0.34,
    shadowRadius: 28,
    shadowOffset: { width: 0, height: 16 },
    elevation: 10,
  },
  cardPressable: {
    flex: 1,
  },
  nextCard: {
    transform: [{ scale: 0.96 }, { translateY: 14 }],
    opacity: 0.5,
  },
  image: {
    width: "100%",
    height: 292,
  },
  cardBody: {
    padding: 20,
    gap: 12,
  },
  kicker: {
    color: palette.accentWarm,
    fontSize: 12,
    fontWeight: "800",
    letterSpacing: 0.8,
    textTransform: "uppercase",
  },
  title: {
    fontSize: 28,
    lineHeight: 33,
    fontWeight: "900",
    color: palette.text,
  },
  subtitle: {
    fontSize: 15,
    lineHeight: 22,
    color: palette.textMuted,
  },
  metaRow: {
    flexDirection: "row",
    flexWrap: "wrap",
    gap: 10,
  },
  metaPill: {
    borderRadius: 999,
    paddingHorizontal: 12,
    paddingVertical: 8,
    backgroundColor: "rgba(255, 255, 255, 0.06)",
    borderWidth: 1,
    borderColor: palette.border,
  },
  metaPillCool: {
    backgroundColor: "rgba(52, 211, 153, 0.12)",
    borderColor: "rgba(52, 211, 153, 0.24)",
  },
  metaPillText: {
    fontSize: 12,
    fontWeight: "700",
    color: palette.textSoft,
  },
  metaPillTextCool: {
    color: "#b4f5dd",
  },
  stopList: {
    gap: 10,
  },
  stopRow: {
    flexDirection: "row",
    alignItems: "center",
    gap: 10,
  },
  stopIndex: {
    width: 24,
    color: palette.accent,
    fontWeight: "900",
  },
  stopText: {
    color: palette.textSoft,
    fontSize: 14,
    flex: 1,
  },
  tapHint: {
    color: palette.accentWarm,
    fontSize: 13,
    fontWeight: "700",
  },
  actions: {
    flexDirection: "row",
    gap: 12,
    paddingBottom: 8,
  },
  actionButton: {
    flex: 1,
  },
  likeBadge: {
    position: "absolute",
    top: 24,
    right: 18,
    borderWidth: 2,
    borderColor: palette.success,
    paddingHorizontal: 14,
    paddingVertical: 8,
    borderRadius: 12,
    transform: [{ rotate: "10deg" }],
    backgroundColor: "rgba(7, 17, 31, 0.84)",
  },
  likeBadgeText: {
    color: "#a7f3d0",
    fontWeight: "900",
    fontSize: 18,
  },
  nopeBadge: {
    position: "absolute",
    top: 24,
    left: 18,
    borderWidth: 2,
    borderColor: palette.danger,
    paddingHorizontal: 14,
    paddingVertical: 8,
    borderRadius: 12,
    transform: [{ rotate: "-10deg" }],
    backgroundColor: "rgba(7, 17, 31, 0.84)",
  },
  nopeBadgeText: {
    color: "#fecdd3",
    fontWeight: "900",
    fontSize: 18,
  },
  emptyWrap: {
    alignItems: "center",
    gap: 10,
    paddingVertical: 36,
  },
  emptyTitle: {
    fontSize: 24,
    fontWeight: "900",
    color: palette.text,
  },
  emptyText: {
    color: palette.textMuted,
    fontSize: 15,
    lineHeight: 22,
    textAlign: "center",
  },
});
