const ISO_DATE_RE = /^(\d{4})-(\d{2})-(\d{2})$/;
const ISO_TIME_RE = /^(\d{2}):(\d{2})$/;

export function buildLocalIsoDateTime(dateText: string, timeText: string) {
  const dateMatch = ISO_DATE_RE.exec(dateText.trim());
  if (!dateMatch) {
    throw new Error("Arrival date must use YYYY-MM-DD.");
  }

  const timeMatch = ISO_TIME_RE.exec(timeText.trim());
  if (!timeMatch) {
    throw new Error("Arrival time must use HH:MM in 24-hour time.");
  }

  const year = Number(dateMatch[1]);
  const month = Number(dateMatch[2]);
  const day = Number(dateMatch[3]);
  const hour = Number(timeMatch[1]);
  const minute = Number(timeMatch[2]);

  if (month < 1 || month > 12) {
    throw new Error("Arrival month must be between 01 and 12.");
  }
  if (hour > 23 || minute > 59) {
    throw new Error("Arrival time must be a real 24-hour time.");
  }

  const value = new Date(year, month - 1, day, hour, minute, 0, 0);
  if (
    value.getFullYear() !== year ||
    value.getMonth() !== month - 1 ||
    value.getDate() !== day
  ) {
    throw new Error("Arrival date must be a real calendar date.");
  }

  return `${formatDateInput(value)}T${formatTimeInput(value)}:00${formatOffset(value)}`;
}

export function formatDateInput(value: Date) {
  return `${value.getFullYear()}-${pad(value.getMonth() + 1)}-${pad(value.getDate())}`;
}

export function formatTimeInput(value: Date) {
  return `${pad(value.getHours())}:${pad(value.getMinutes())}`;
}

export function isoToLocalInputs(isoText: string) {
  const value = new Date(isoText);
  if (Number.isNaN(value.getTime())) {
    throw new Error("Suggested arrival time was not a valid ISO timestamp.");
  }

  return {
    date: formatDateInput(value),
    time: formatTimeInput(value),
  };
}

function formatOffset(value: Date) {
  const offsetMinutes = -value.getTimezoneOffset();
  const sign = offsetMinutes >= 0 ? "+" : "-";
  const absolute = Math.abs(offsetMinutes);
  return `${sign}${pad(Math.floor(absolute / 60))}:${pad(absolute % 60)}`;
}

function pad(value: number) {
  return String(value).padStart(2, "0");
}
