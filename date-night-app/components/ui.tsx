import React from "react";
import {
  Pressable,
  ScrollView,
  StyleProp,
  StyleSheet,
  Text,
  View,
  ViewStyle,
} from "react-native";
import { SafeAreaView } from "react-native-safe-area-context";

export const palette = {
  bg: "#07111f",
  panel: "rgba(11, 23, 40, 0.84)",
  panelSoft: "rgba(16, 31, 54, 0.72)",
  border: "rgba(255, 255, 255, 0.10)",
  borderStrong: "rgba(255, 255, 255, 0.18)",
  text: "#f8fafc",
  textMuted: "#9fb2c8",
  textSoft: "#cbd5e1",
  accent: "#ff7a59",
  accentWarm: "#f9c74f",
  accentCool: "#34d399",
  accentRose: "#fb7185",
  success: "#22c55e",
  danger: "#fb7185",
};

type ShellProps = {
  children: React.ReactNode;
  scroll?: boolean;
  style?: StyleProp<ViewStyle>;
  contentContainerStyle?: StyleProp<ViewStyle>;
};

export function ScreenShell({
  children,
  scroll = false,
  style,
  contentContainerStyle,
}: ShellProps) {
  if (scroll) {
    return (
      <View style={[styles.screen, style]}>
        <BackgroundOrbs />
        <SafeAreaView edges={["top"]} style={styles.safeArea}>
          <ScrollView
            contentContainerStyle={[styles.scrollContent, contentContainerStyle]}
            showsVerticalScrollIndicator={false}
          >
            {children}
          </ScrollView>
        </SafeAreaView>
      </View>
    );
  }

  return (
    <View style={[styles.screen, style]}>
      <BackgroundOrbs />
      <SafeAreaView edges={["top"]} style={styles.safeArea}>
        <View style={[styles.fillContent, contentContainerStyle]}>{children}</View>
      </SafeAreaView>
    </View>
  );
}

export function BackgroundOrbs() {
  return (
    <View pointerEvents="none" style={StyleSheet.absoluteFill}>
      <View style={[styles.orb, styles.orbLarge]} />
      <View style={[styles.orb, styles.orbMedium]} />
      <View style={[styles.orb, styles.orbSmall]} />
      <View style={styles.gridGlow} />
    </View>
  );
}

export function SurfaceCard({
  children,
  style,
}: {
  children: React.ReactNode;
  style?: StyleProp<ViewStyle>;
}) {
  return <View style={[styles.surfaceCard, style]}>{children}</View>;
}

export function Eyebrow({
  children,
  tone = "default",
}: {
  children: React.ReactNode;
  tone?: "default" | "warm" | "success";
}) {
  return <Text style={[styles.eyebrow, eyebrowTone[tone]]}>{children}</Text>;
}

export function SectionTitle({
  title,
  subtitle,
}: {
  title: string;
  subtitle?: string;
}) {
  return (
    <View style={styles.sectionTitleWrap}>
      <Text style={styles.sectionTitle}>{title}</Text>
      {subtitle ? <Text style={styles.sectionSubtitle}>{subtitle}</Text> : null}
    </View>
  );
}

export function SelectChip({
  label,
  selected,
  onPress,
}: {
  label: string;
  selected?: boolean;
  onPress?: () => void;
}) {
  return (
    <Pressable
      disabled={!onPress}
      onPress={onPress}
      style={[styles.chip, selected && styles.chipSelected]}
    >
      <Text style={[styles.chipText, selected && styles.chipTextSelected]}>{label}</Text>
    </Pressable>
  );
}

export function ActionButton({
  label,
  onPress,
  variant = "primary",
  style,
}: {
  label: string;
  onPress: () => void;
  variant?: "primary" | "secondary" | "ghost";
  style?: StyleProp<ViewStyle>;
}) {
  return (
    <Pressable
      onPress={onPress}
      style={[styles.actionButton, actionButtonVariants[variant], style]}
    >
      <Text style={[styles.actionButtonText, actionButtonTextVariants[variant]]}>
        {label}
      </Text>
    </Pressable>
  );
}

const eyebrowTone = StyleSheet.create({
  default: {
    color: palette.accentWarm,
    backgroundColor: "rgba(249, 199, 79, 0.14)",
  },
  warm: {
    color: palette.accent,
    backgroundColor: "rgba(255, 122, 89, 0.14)",
  },
  success: {
    color: palette.accentCool,
    backgroundColor: "rgba(52, 211, 153, 0.14)",
  },
});

const actionButtonVariants = StyleSheet.create({
  primary: {
    backgroundColor: palette.accent,
    borderColor: "rgba(255, 151, 124, 0.55)",
  },
  secondary: {
    backgroundColor: "rgba(255, 255, 255, 0.06)",
    borderColor: palette.borderStrong,
  },
  ghost: {
    backgroundColor: "transparent",
    borderColor: "transparent",
  },
});

const actionButtonTextVariants = StyleSheet.create({
  primary: {
    color: "#1b1120",
  },
  secondary: {
    color: palette.text,
  },
  ghost: {
    color: palette.textMuted,
  },
});

const styles = StyleSheet.create({
  screen: {
    flex: 1,
    backgroundColor: palette.bg,
  },
  safeArea: {
    flex: 1,
  },
  fillContent: {
    flex: 1,
    paddingHorizontal: 20,
    paddingBottom: 18,
  },
  scrollContent: {
    paddingHorizontal: 20,
    paddingBottom: 32,
    gap: 16,
  },
  orb: {
    position: "absolute",
    borderRadius: 999,
  },
  orbLarge: {
    width: 280,
    height: 280,
    top: -72,
    right: -48,
    backgroundColor: "rgba(255, 122, 89, 0.24)",
  },
  orbMedium: {
    width: 220,
    height: 220,
    top: 180,
    left: -110,
    backgroundColor: "rgba(52, 211, 153, 0.14)",
  },
  orbSmall: {
    width: 180,
    height: 180,
    bottom: 70,
    right: -70,
    backgroundColor: "rgba(249, 199, 79, 0.14)",
  },
  gridGlow: {
    ...StyleSheet.absoluteFillObject,
    backgroundColor: "rgba(255, 255, 255, 0.01)",
    borderTopWidth: 1,
    borderTopColor: "rgba(255, 255, 255, 0.02)",
  },
  surfaceCard: {
    backgroundColor: palette.panel,
    borderRadius: 28,
    borderWidth: 1,
    borderColor: palette.border,
    padding: 18,
    shadowColor: "#020617",
    shadowOpacity: 0.32,
    shadowRadius: 28,
    shadowOffset: { width: 0, height: 14 },
    elevation: 10,
  },
  eyebrow: {
    alignSelf: "flex-start",
    overflow: "hidden",
    paddingHorizontal: 12,
    paddingVertical: 7,
    borderRadius: 999,
    fontSize: 12,
    fontWeight: "700",
    letterSpacing: 0.8,
    textTransform: "uppercase",
  },
  sectionTitleWrap: {
    gap: 4,
  },
  sectionTitle: {
    fontSize: 24,
    fontWeight: "800",
    color: palette.text,
  },
  sectionSubtitle: {
    fontSize: 14,
    color: palette.textMuted,
    lineHeight: 20,
  },
  chip: {
    paddingHorizontal: 14,
    paddingVertical: 11,
    borderRadius: 999,
    borderWidth: 1,
    borderColor: palette.border,
    backgroundColor: "rgba(255, 255, 255, 0.05)",
  },
  chipSelected: {
    backgroundColor: "rgba(255, 122, 89, 0.18)",
    borderColor: "rgba(255, 151, 124, 0.45)",
  },
  chipText: {
    color: palette.textMuted,
    fontSize: 14,
    fontWeight: "600",
  },
  chipTextSelected: {
    color: palette.text,
  },
  actionButton: {
    minHeight: 52,
    paddingHorizontal: 18,
    paddingVertical: 14,
    borderRadius: 999,
    borderWidth: 1,
    justifyContent: "center",
    alignItems: "center",
  },
  actionButtonText: {
    fontSize: 15,
    fontWeight: "800",
    letterSpacing: 0.2,
  },
});
