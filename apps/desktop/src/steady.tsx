import { useEffect, useRef, useState } from "react";
import { eta, speed } from "./format";

export function useSteady<T>(value: T, intervalMs = 1000): T {
  const [shown, setShown] = useState(value);
  const latest = useRef(value);
  latest.current = value;
  useEffect(() => {
    const id = window.setInterval(() => {
      setShown((current) => (Object.is(current, latest.current) ? current : latest.current));
    }, intervalMs);
    return () => window.clearInterval(id);
  }, [intervalMs]);
  return shown;
}

export function SteadySpeed({ value }: { value: number }) {
  const shown = useSteady(value);
  return <>{speed(shown)}</>;
}

export function SteadyEta({ value }: { value: number | null }) {
  const shown = useSteady(value);
  return <>{eta(shown)}</>;
}
