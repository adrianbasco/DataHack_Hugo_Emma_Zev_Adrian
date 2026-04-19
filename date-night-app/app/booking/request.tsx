import { useEffect, useMemo, useState, type ReactNode } from "react";
import { ActivityIndicator, StyleSheet, Text, TextInput, View } from "react-native";
import { useLocalSearchParams, useRouter } from "expo-router";
import { createRestaurantBooking } from "../../lib/api";
import {
  buildLocalIsoDateTime,
  formatDateInput,
  isoToLocalInputs,
} from "../../lib/datetime";
import { getPlanById } from "../../lib/storage";
import { Plan } from "../../lib/types";
import {
  ActionButton,
  Eyebrow,
  ScreenShell,
  SurfaceCard,
  palette,
} from "../../components/ui";

export default function BookingRequestScreen() {
  const router = useRouter();
  const { planId } = useLocalSearchParams<{ planId?: string }>();
  const [plan, setPlan] = useState<Plan | null>(null);
  const [loadingPlan, setLoadingPlan] = useState(true);
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | undefined>();

  const today = useMemo(() => new Date(), []);
  const initialDate = formatDateInput(today);
  const initialTime = "19:00";

  const [restaurantName, setRestaurantName] = useState("");
  const [restaurantPhoneNumber, setRestaurantPhoneNumber] = useState("");
  const [restaurantAddress, setRestaurantAddress] = useState("");
  const [bookingName, setBookingName] = useState("");
  const [customerPhoneNumber, setCustomerPhoneNumber] = useState("");
  const [partySize, setPartySize] = useState("2");
  const [arrivalDate, setArrivalDate] = useState(initialDate);
  const [arrivalTime, setArrivalTime] = useState(initialTime);
  const [acceptableWindow, setAcceptableWindow] = useState("15");
  const [dietaryConstraints, setDietaryConstraints] = useState("");
  const [accessibilityConstraints, setAccessibilityConstraints] = useState("");
  const [specialOccasion, setSpecialOccasion] = useState("");
  const [notes, setNotes] = useState("");

  useEffect(() => {
    let active = true;

    async function hydratePlan() {
      if (!planId) {
        setLoadingPlan(false);
        return;
      }

      const foundPlan = await getPlanById(planId);
      if (!active) {
        return;
      }

      setPlan(foundPlan ?? null);
      setRestaurantName(foundPlan?.bookingContext?.restaurantName || "");
      setRestaurantPhoneNumber(foundPlan?.bookingContext?.restaurantPhoneNumber || "");
      setRestaurantAddress(foundPlan?.bookingContext?.restaurantAddress || "");
      setPartySize(String(foundPlan?.bookingContext?.partySize || 2));
      if (foundPlan?.bookingContext?.suggestedArrivalTimeIso) {
        try {
          const suggested = isoToLocalInputs(foundPlan.bookingContext.suggestedArrivalTimeIso);
          setArrivalDate(suggested.date);
          setArrivalTime(suggested.time);
        } catch (parseError) {
          console.error("Failed to prefill suggested booking time from plan context.", parseError);
        }
      }
      setLoadingPlan(false);
    }

    void hydratePlan();

    return () => {
      active = false;
    };
  }, [planId]);

  async function handleSubmit() {
    const trimmedRestaurantName = restaurantName.trim();
    const trimmedRestaurantPhone = restaurantPhoneNumber.trim();
    const trimmedBookingName = bookingName.trim();
    const trimmedCustomerPhone = customerPhoneNumber.trim();
    const parsedPartySize = Number(partySize);
    const parsedWindow = acceptableWindow.trim() ? Number(acceptableWindow) : undefined;

    if (!trimmedRestaurantName) {
      setError("Restaurant name is required.");
      return;
    }
    if (!trimmedBookingName) {
      setError("Booking name is required.");
      return;
    }
    if (!isE164PhoneNumber(trimmedRestaurantPhone)) {
      setError("Restaurant phone number must use E.164 format, for example +61290000000.");
      return;
    }
    if (trimmedCustomerPhone && !isE164PhoneNumber(trimmedCustomerPhone)) {
      setError("Customer phone number must use E.164 format when provided.");
      return;
    }
    if (!Number.isFinite(parsedPartySize) || parsedPartySize <= 0) {
      setError("Party size must be a positive whole number.");
      return;
    }
    if (
      parsedWindow !== undefined &&
      (!Number.isFinite(parsedWindow) || parsedWindow < 0)
    ) {
      setError("Acceptable time window must be zero or a positive number of minutes.");
      return;
    }

    let arrivalTimeIso: string;
    try {
      arrivalTimeIso = buildLocalIsoDateTime(arrivalDate, arrivalTime);
    } catch (validationError) {
      setError(
        validationError instanceof Error
          ? validationError.message
          : "Arrival date and time were not valid."
      );
      return;
    }

    try {
      setSubmitting(true);
      setError(undefined);

      const job = await createRestaurantBooking({
        restaurantName: trimmedRestaurantName,
        restaurantPhoneNumber: trimmedRestaurantPhone,
        restaurantAddress: restaurantAddress || undefined,
        arrivalTimeIso,
        partySize: parsedPartySize,
        bookingName: trimmedBookingName,
        customerPhoneNumber: trimmedCustomerPhone || undefined,
        dietaryConstraints: dietaryConstraints || undefined,
        accessibilityConstraints: accessibilityConstraints || undefined,
        specialOccasion: specialOccasion || undefined,
        notes: notes || undefined,
        acceptableTimeWindowMinutes: parsedWindow,
        planId: plan?.id || planId || undefined,
      });

      router.replace({
        pathname: "/booking/[status]",
        params: {
          status: job.status,
          callId: job.callId,
        },
      });
    } catch (submitError) {
      setError(
        submitError instanceof Error
          ? submitError.message
          : "Booking request could not be submitted."
      );
    } finally {
      setSubmitting(false);
    }
  }

  if (loadingPlan) {
    return (
      <ScreenShell contentContainerStyle={styles.centered}>
        <SurfaceCard style={styles.centerCard}>
          <ActivityIndicator color={palette.accent} />
          <Text style={styles.centerTitle}>Loading booking context</Text>
        </SurfaceCard>
      </ScreenShell>
    );
  }

  return (
    <ScreenShell scroll>
      <View style={styles.hero}>
        <Eyebrow tone="warm">Restaurant booking</Eyebrow>
        <Text style={styles.title}>Submit a restaurant booking request.</Text>
        <Text style={styles.subtitle}>
          This screen is shaped around the backend booking service, so once the endpoint is live it can pass a real reservation brief through directly.
        </Text>
      </View>

      {plan ? (
        <SurfaceCard style={styles.planCard}>
          <Text style={styles.planLabel}>From plan</Text>
          <Text style={styles.planTitle}>{plan.title}</Text>
          <Text style={styles.planText}>{plan.hook}</Text>
        </SurfaceCard>
      ) : null}

      {error ? (
        <SurfaceCard style={styles.errorCard}>
          <Text style={styles.errorTitle}>Booking request failed</Text>
          <Text style={styles.errorText}>{error}</Text>
        </SurfaceCard>
      ) : null}

      <SurfaceCard style={styles.formCard}>
        <Field label="Restaurant name">
          <TextInput
            style={styles.input}
            value={restaurantName}
            onChangeText={setRestaurantName}
            placeholder="Restaurant name"
            placeholderTextColor={palette.textMuted}
          />
        </Field>

        <Field label="Restaurant phone number">
          <TextInput
            style={styles.input}
            value={restaurantPhoneNumber}
            onChangeText={setRestaurantPhoneNumber}
            placeholder="+61290000000"
            placeholderTextColor={palette.textMuted}
          />
        </Field>

        <Field label="Restaurant address">
          <TextInput
            style={styles.input}
            value={restaurantAddress}
            onChangeText={setRestaurantAddress}
            placeholder="Optional address"
            placeholderTextColor={palette.textMuted}
          />
        </Field>

        <View style={styles.row}>
          <Field label="Booking name">
            <TextInput
              style={styles.input}
              value={bookingName}
              onChangeText={setBookingName}
              placeholder="Your name"
              placeholderTextColor={palette.textMuted}
            />
          </Field>

          <Field label="Customer phone">
            <TextInput
              style={styles.input}
              value={customerPhoneNumber}
              onChangeText={setCustomerPhoneNumber}
              placeholder="Optional contact"
              placeholderTextColor={palette.textMuted}
            />
          </Field>
        </View>

        <View style={styles.row}>
          <Field label="Arrival date">
            <TextInput
              style={styles.input}
              value={arrivalDate}
              onChangeText={setArrivalDate}
              placeholder="YYYY-MM-DD"
              placeholderTextColor={palette.textMuted}
            />
          </Field>

          <Field label="Arrival time">
            <TextInput
              style={styles.input}
              value={arrivalTime}
              onChangeText={setArrivalTime}
              placeholder="19:00"
              placeholderTextColor={palette.textMuted}
            />
          </Field>
        </View>

        <View style={styles.row}>
          <Field label="Party size">
            <TextInput
              style={styles.input}
              value={partySize}
              onChangeText={setPartySize}
              keyboardType="numeric"
              placeholder="2"
              placeholderTextColor={palette.textMuted}
            />
          </Field>

          <Field label="Acceptable time window">
            <TextInput
              style={styles.input}
              value={acceptableWindow}
              onChangeText={setAcceptableWindow}
              keyboardType="numeric"
              placeholder="15"
              placeholderTextColor={palette.textMuted}
            />
          </Field>
        </View>

        <Field label="Dietary constraints">
          <TextInput
            style={styles.input}
            value={dietaryConstraints}
            onChangeText={setDietaryConstraints}
            placeholder="Optional dietary notes"
            placeholderTextColor={palette.textMuted}
          />
        </Field>

        <Field label="Accessibility constraints">
          <TextInput
            style={styles.input}
            value={accessibilityConstraints}
            onChangeText={setAccessibilityConstraints}
            placeholder="Optional accessibility notes"
            placeholderTextColor={palette.textMuted}
          />
        </Field>

        <Field label="Special occasion">
          <TextInput
            style={styles.input}
            value={specialOccasion}
            onChangeText={setSpecialOccasion}
            placeholder="Anniversary, birthday..."
            placeholderTextColor={palette.textMuted}
          />
        </Field>

        <Field label="Notes">
          <TextInput
            style={[styles.input, styles.notesInput]}
            value={notes}
            onChangeText={setNotes}
            placeholder="Extra notes for the booking call..."
            placeholderTextColor={palette.textMuted}
            multiline
          />
        </Field>

        <ActionButton
          label={submitting ? "Submitting..." : "Submit booking request"}
          onPress={handleSubmit}
          disabled={submitting}
        />
      </SurfaceCard>
    </ScreenShell>
  );
}

function Field({
  label,
  children,
}: {
  label: string;
  children: ReactNode;
}) {
  return (
    <View style={styles.field}>
      <Text style={styles.fieldLabel}>{label}</Text>
      {children}
    </View>
  );
}

function isE164PhoneNumber(value: string) {
  return /^\+[1-9]\d{6,14}$/.test(value);
}

const styles = StyleSheet.create({
  centered: {
    flex: 1,
    justifyContent: "center",
  },
  centerCard: {
    gap: 10,
    alignItems: "center",
  },
  centerTitle: {
    color: palette.text,
    fontSize: 22,
    fontWeight: "800",
  },
  hero: {
    gap: 8,
  },
  title: {
    color: palette.text,
    fontSize: 34,
    lineHeight: 40,
    fontWeight: "900",
  },
  subtitle: {
    color: palette.textMuted,
    lineHeight: 23,
    fontSize: 15,
  },
  planCard: {
    gap: 6,
  },
  planLabel: {
    color: palette.accentWarm,
    fontSize: 12,
    fontWeight: "800",
    letterSpacing: 0.7,
    textTransform: "uppercase",
  },
  planTitle: {
    color: palette.text,
    fontSize: 20,
    fontWeight: "800",
  },
  planText: {
    color: palette.textSoft,
    lineHeight: 21,
  },
  errorCard: {
    gap: 6,
  },
  errorTitle: {
    color: "#fecdd3",
    fontSize: 18,
    fontWeight: "800",
  },
  errorText: {
    color: palette.textMuted,
    lineHeight: 21,
  },
  formCard: {
    gap: 14,
  },
  row: {
    flexDirection: "row",
    gap: 12,
    flexWrap: "wrap",
  },
  field: {
    flex: 1,
    minWidth: 160,
    gap: 8,
  },
  fieldLabel: {
    color: palette.textSoft,
    fontSize: 14,
    fontWeight: "700",
  },
  input: {
    borderWidth: 1,
    borderColor: palette.border,
    backgroundColor: palette.panelSoft,
    borderRadius: 18,
    paddingHorizontal: 16,
    paddingVertical: 14,
    fontSize: 15,
    color: palette.text,
  },
  notesInput: {
    minHeight: 96,
    textAlignVertical: "top",
  },
});
