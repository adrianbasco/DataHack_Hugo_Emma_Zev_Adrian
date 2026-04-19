import { StatusBar } from "expo-status-bar";
import { type PropsWithChildren } from "react";
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
  bgDeep: "#040b13",
  panel: "rgba(11, 24, 39, 0.92)",
  panelSoft: "rgba(18, 36, 58, 0.9)",
  border: "rgba(148, 163, 184, 0.16)",
  text: "#f8fafc",
  textSoft: "#dbe7f5",
  textMuted: "#94a3b8",
  accent: "#67e8f9",
  accentWarm: "#ff7a59",
  success: "#34d399",
  danger: "#fb7185",
};

type ScreenShellProps = PropsWithChildren<{
  scroll?: boolean;
  style?: StyleProp<ViewStyle>;
  contentContainerStyle?: StyleProp<ViewStyle>;
}>;

type EyebrowTone = "default" | "warm" | "success";

type ActionButtonProps = {
  label: string;
  onPress?: () => void;
  variant?: "primary" | "secondary";
  disabled?: boolean;
  style?: StyleProp<ViewStyle>;
};

type SelectChipProps = {
  label: string;
  selected?: boolean;
  onPress?: () => void;
  disabled?: boolean;
  style?: StyleProp<ViewStyle>;
};

export function ScreenShell({
  children,
  scroll = false,
  style,
  contentContainerStyle,
}: ScreenShellProps) {
  const content = scroll ? (
    <ScrollView
      style={styles.fill}
      contentContainerStyle={[styles.scrollContent, contentContainerStyle]}
      showsVerticalScrollIndicator={false}
    >
      {children}
    </ScrollView>
  ) : (
    <View style={[styles.content, contentContainerStyle]}>{children}</View>
  );

  return (
    <SafeAreaView style={[styles.safeArea, style]} edges={["top", "left", "right"]}>
      <StatusBar style="light" />
      <View pointerEvents="none" style={styles.backdrop}>
        <View style={[styles.orb, styles.orbCool]} />
        <View style={[styles.orb, styles.orbWarm]} />
        <View style={[styles.orb, styles.orbMint]} />
        <View style={styles.grid} />
      </View>
      {content}
    </SafeAreaView>
  );
}

export function SurfaceCard({
  children,
  style,
}: PropsWithChildren<{ style?: StyleProp<ViewStyle> }>) {
  return <View style={[styles.surfaceCard, style]}>{children}</View>;
}

export function Eyebrow({
  children,
  tone = "default",
}: PropsWithChildren<{ tone?: EyebrowTone }>) {
  return (
    <Text
      style={[
        styles.eyebrow,
        tone === "warm" && styles.eyebrowWarm,
        tone === "success" && styles.eyebrowSuccess,
      ]}
    >
      {children}
    </Text>
  );
}

export function SelectChip({
  label,
  selected = false,
  onPress,
  disabled = false,
  style,
}: SelectChipProps) {
  const chipStyles = [
    styles.chip,
    selected ? styles.chipSelected : styles.chipIdle,
    disabled && styles.chipDisabled,
    style,
  ];
  const labelStyles = [
    styles.chipText,
    selected ? styles.chipTextSelected : styles.chipTextIdle,
    disabled && styles.chipTextDisabled,
  ];

  if (!onPress) {
    return (
      <View style={chipStyles}>
        <Text style={labelStyles}>{label}</Text>
      </View>
    );
  }

  return (
    <Pressable
      onPress={onPress}
      disabled={disabled}
      style={({ pressed }) => [chipStyles, pressed && !disabled && styles.pressableDown]}
    >
      <Text style={labelStyles}>{label}</Text>
    </Pressable>
  );
}

export function ActionButton({
  label,
  onPress,
  variant = "primary",
  disabled = false,
  style,
}: ActionButtonProps) {
  return (
    <Pressable
      onPress={onPress}
      disabled={disabled}
      style={({ pressed }) => [
        styles.button,
        variant === "primary" ? styles.buttonPrimary : styles.buttonSecondary,
        disabled && styles.buttonDisabled,
        pressed && !disabled && styles.pressableDown,
        style,
      ]}
    >
      <Text
        style={[
          styles.buttonText,
          variant === "primary" ? styles.buttonTextPrimary : styles.buttonTextSecondary,
          disabled && styles.buttonTextDisabled,
        ]}
      >
        {label}
      </Text>
    </Pressable>
  );
}

const styles = StyleSheet.create({
  fill: {
    flex: 1,
  },
  safeArea: {
    flex: 1,
    backgroundColor: palette.bg,
    overflow: "hidden",
  },
  backdrop: {
    ...StyleSheet.absoluteFillObject,
    backgroundColor: palette.bg,
  },
  orb: {
    position: "absolute",
    borderRadius: 999,
  },
  orbCool: {
    width: 320,
    height: 320,
    right: -70,
    top: -40,
    backgroundColor: "rgba(103, 232, 249, 0.12)",
  },
  orbWarm: {
    width: 260,
    height: 260,
    left: -90,
    top: 240,
    backgroundColor: "rgba(255, 122, 89, 0.12)",
  },
  orbMint: {
    width: 240,
    height: 240,
    right: 20,
    bottom: 80,
    backgroundColor: "rgba(52, 211, 153, 0.08)",
  },
  grid: {
    ...StyleSheet.absoluteFillObject,
    opacity: 0.08,
    borderTopWidth: 1,
    borderBottomWidth: 1,
    borderColor: "rgba(148, 163, 184, 0.08)",
  },
  content: {
    flex: 1,
    paddingHorizontal: 18,
    paddingTop: 8,
    paddingBottom: 24,
    gap: 16,
  },
  scrollContent: {
    paddingHorizontal: 18,
    paddingTop: 8,
    paddingBottom: 28,
    gap: 16,
  },
  surfaceCard: {
    borderRadius: 28,
    padding: 18,
    backgroundColor: palette.panel,
    borderWidth: 1,
    borderColor: palette.border,
    shadowColor: "#020617",
    shadowOpacity: 0.22,
    shadowRadius: 18,
    shadowOffset: { width: 0, height: 12 },
    elevation: 8,
  },
  eyebrow: {
    color: palette.accent,
    fontSize: 12,
    fontWeight: "800",
    letterSpacing: 1,
    textTransform: "uppercase",
  },
  eyebrowWarm: {
    color: palette.accentWarm,
  },
  eyebrowSuccess: {
    color: "#9cf2cf",
  },
  chip: {
    minHeight: 38,
    paddingHorizontal: 14,
    paddingVertical: 9,
    borderRadius: 999,
    borderWidth: 1,
    justifyContent: "center",
    alignItems: "center",
  },
  chipIdle: {
    backgroundColor: "rgba(255, 255, 255, 0.05)",
    borderColor: palette.border,
  },
  chipSelected: {
    backgroundColor: "rgba(255, 122, 89, 0.16)",
    borderColor: "rgba(255, 151, 124, 0.34)",
  },
  chipDisabled: {
    opacity: 0.55,
  },
  chipText: {
    fontSize: 13,
    fontWeight: "700",
  },
  chipTextIdle: {
    color: palette.textSoft,
  },
  chipTextSelected: {
    color: palette.text,
  },
  chipTextDisabled: {
    color: palette.textMuted,
  },
  button: {
    minHeight: 52,
    borderRadius: 18,
    paddingHorizontal: 16,
    paddingVertical: 13,
    justifyContent: "center",
    alignItems: "center",
    borderWidth: 1,
  },
  buttonPrimary: {
    backgroundColor: palette.accentWarm,
    borderColor: "rgba(255, 190, 171, 0.5)",
  },
  buttonSecondary: {
    backgroundColor: "rgba(255, 255, 255, 0.05)",
    borderColor: palette.border,
  },
  buttonDisabled: {
    opacity: 0.55,
  },
  buttonText: {
    fontSize: 15,
    fontWeight: "800",
    textAlign: "center",
  },
  buttonTextPrimary: {
    color: "#101826",
  },
  buttonTextSecondary: {
    color: palette.text,
  },
  buttonTextDisabled: {
    color: palette.textMuted,
  },
  pressableDown: {
    transform: [{ translateY: 1 }],
  },
});
