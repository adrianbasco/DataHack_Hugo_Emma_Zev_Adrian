import React, { useMemo, useRef, useState } from "react";
import {
  View,
  Text,
  Image,
  Pressable,
  StyleSheet,
  Animated,
  PanResponder,
  Dimensions,
} from "react-native";
import { Plan } from "../lib/types";

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
    outputRange: ["-18deg", "0deg", "18deg"],
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
      onMoveShouldSetPanResponder: (_evt, gestureState) => {
        return Math.abs(gestureState.dx) > 8 || Math.abs(gestureState.dy) > 8;
      },
      onPanResponderMove: (_evt, gestureState) => {
        position.setValue({ x: gestureState.dx, y: gestureState.dy * 0.2 });
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
      <View style={styles.emptyWrap}>
        <Text style={styles.emptyTitle}>No more plans</Text>
        <Text style={styles.emptyText}>
          You’ve reached the end of the deck.
        </Text>
      </View>
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
              <Text style={styles.title}>{nextPlan.title}</Text>
              <Text style={styles.subtitle}>{nextPlan.vibeLine}</Text>
            </View>
          </View>
        ) : null}

        <Animated.View
          style={[styles.card, animatedCardStyle]}
          {...panResponder.panHandlers}
        >
          <Pressable style={{ flex: 1 }} onPress={() => onOpenPlan(currentPlan)}>
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
              <Text style={styles.title}>{currentPlan.title}</Text>
              <Text style={styles.subtitle}>{currentPlan.vibeLine}</Text>

              <View style={styles.metaRow}>
                <Text style={styles.metaText}>{currentPlan.durationLabel}</Text>
                <Text style={styles.metaDot}>•</Text>
                <Text style={styles.metaText}>{currentPlan.costBand}</Text>
                {currentPlan.weather ? (
                  <>
                    <Text style={styles.metaDot}>•</Text>
                    <Text style={styles.metaText}>{currentPlan.weather}</Text>
                  </>
                ) : null}
              </View>

              <View style={styles.stopList}>
                {currentPlan.stops.slice(0, 3).map((stop, index) => (
                  <Text key={stop.id} style={styles.stopText}>
                    {index + 1}. {stop.name}
                  </Text>
                ))}
              </View>

              <Text style={styles.tapHint}>Tap card for details</Text>
            </View>
          </Pressable>
        </Animated.View>
      </View>

      <View style={styles.actions}>
        <Pressable style={styles.skipButton} onPress={() => forceSwipe("left")}>
          <Text style={styles.skipButtonText}>Skip</Text>
        </Pressable>

        <Pressable style={styles.saveButton} onPress={() => forceSwipe("right")}>
          <Text style={styles.saveButtonText}>Save</Text>
        </Pressable>
      </View>
    </View>
  );
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
  },
  deckArea: {
    flex: 1,
    justifyContent: "center",
    alignItems: "center",
    minHeight: 560,
  },
  card: {
    width: "100%",
    maxWidth: 380,
    backgroundColor: "white",
    borderRadius: 28,
    overflow: "hidden",
    position: "absolute",
    shadowColor: "#000",
    shadowOpacity: 0.12,
    shadowRadius: 16,
    shadowOffset: { width: 0, height: 6 },
    elevation: 6,
  },
  nextCard: {
    transform: [{ scale: 0.96 }, { translateY: 8 }],
    opacity: 0.55,
  },
  image: {
    width: "100%",
    height: 280,
  },
  cardBody: {
    padding: 18,
  },
  title: {
    fontSize: 24,
    fontWeight: "700",
    color: "#881337",
    marginBottom: 6,
  },
  subtitle: {
    fontSize: 15,
    color: "#475569",
    marginBottom: 12,
  },
  metaRow: {
    flexDirection: "row",
    flexWrap: "wrap",
    alignItems: "center",
    marginBottom: 12,
  },
  metaText: {
    fontSize: 13,
    color: "#64748b",
  },
  metaDot: {
    color: "#cbd5e1",
    marginHorizontal: 6,
  },
  stopList: {
    gap: 6,
    marginBottom: 14,
  },
  stopText: {
    color: "#334155",
    fontSize: 14,
  },
  tapHint: {
    marginTop: 4,
    color: "#be185d",
    fontSize: 13,
    fontWeight: "600",
  },
  actions: {
    flexDirection: "row",
    gap: 12,
    paddingTop: 16,
    paddingBottom: 10,
  },
  skipButton: {
    flex: 1,
    backgroundColor: "white",
    borderRadius: 999,
    paddingVertical: 15,
    borderWidth: 1,
    borderColor: "#e2e8f0",
  },
  skipButtonText: {
    textAlign: "center",
    color: "#475569",
    fontWeight: "700",
    fontSize: 16,
  },
  saveButton: {
    flex: 1,
    backgroundColor: "#ec4899",
    borderRadius: 999,
    paddingVertical: 15,
  },
  saveButtonText: {
    textAlign: "center",
    color: "white",
    fontWeight: "700",
    fontSize: 16,
  },
  likeBadge: {
    position: "absolute",
    top: 24,
    right: 20,
    borderWidth: 3,
    borderColor: "#16a34a",
    paddingHorizontal: 14,
    paddingVertical: 8,
    borderRadius: 12,
    transform: [{ rotate: "12deg" }],
    backgroundColor: "rgba(255,255,255,0.9)",
  },
  likeBadgeText: {
    color: "#16a34a",
    fontWeight: "800",
    fontSize: 18,
  },
  nopeBadge: {
    position: "absolute",
    top: 24,
    left: 20,
    borderWidth: 3,
    borderColor: "#ef4444",
    paddingHorizontal: 14,
    paddingVertical: 8,
    borderRadius: 12,
    transform: [{ rotate: "-12deg" }],
    backgroundColor: "rgba(255,255,255,0.9)",
  },
  nopeBadgeText: {
    color: "#ef4444",
    fontWeight: "800",
    fontSize: 18,
  },
  emptyWrap: {
    flex: 1,
    justifyContent: "center",
    alignItems: "center",
    paddingVertical: 80,
  },
  emptyTitle: {
    fontSize: 24,
    fontWeight: "700",
    marginBottom: 8,
    color: "#881337",
  },
  emptyText: {
    color: "#64748b",
    fontSize: 15,
  },
});