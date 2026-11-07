import { config, type IconDefinition } from "@fortawesome/fontawesome-svg-core";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import "@fortawesome/fontawesome-svg-core/styles.css";
import type { ButtonHTMLAttributes, ReactNode } from "react";
import {
  faArrowDown,
  faCircle,
  faCircleCheck,
  faCircleExclamation,
  faClock,
  faCube,
  faFile,
  faFileLines,
  faFileZipper,
  faFolder,
  faImage,
  faLayerGroup,
  faList,
  faMusic,
  faPause,
  faStop,
  faArrowsRotate,
  faVideo,
} from "@fortawesome/free-solid-svg-icons";

config.autoAddCss = false;

const VIDEO = new Set(["mp4", "mkv", "avi", "mov", "webm", "m4v", "flv", "wmv", "mpg", "mpeg"]);
const AUDIO = new Set(["mp3", "flac", "wav", "aac", "m4a", "ogg", "wma"]);
const DOCS = new Set(["pdf", "doc", "docx", "xls", "xlsx", "ppt", "pptx", "txt", "rtf", "odt", "csv"]);
const ARCHIVES = new Set(["zip", "rar", "7z", "tar", "gz", "bz2", "xz", "iso"]);
const APPS = new Set(["exe", "msi", "dmg", "pkg", "appimage", "deb", "rpm", "apk"]);
const IMAGES = new Set(["png", "jpg", "jpeg", "gif", "webp", "svg", "heic", "bmp", "ico"]);

export function Icon({ icon }: { icon: IconDefinition }) {
  return <FontAwesomeIcon icon={icon} fixedWidth />;
}

export function IconButton({
  icon,
  children,
  type = "button",
  ...props
}: ButtonHTMLAttributes<HTMLButtonElement> & { icon: IconDefinition; children?: ReactNode }) {
  return (
    <button type={type} {...props}>
      <Icon icon={icon} />
      {children != null && <span>{children}</span>}
    </button>
  );
}

export function libraryIcon(kind: string): IconDefinition {
  switch (kind) {
    case "all":
      return faList;
    case "downloading":
      return faArrowDown;
    case "queued":
      return faClock;
    case "completed":
      return faCircleCheck;
    case "paused":
      return faPause;
    case "failed":
      return faCircleExclamation;
    default:
      return faFolder;
  }
}

export function categoryIcon(name: string): IconDefinition {
  switch (name.toLowerCase()) {
    case "videos":
      return faVideo;
    case "music":
      return faMusic;
    case "documents":
      return faFileLines;
    case "compressed":
      return faFileZipper;
    case "applications":
      return faCube;
    default:
      return faFolder;
  }
}

export function queueIcon(name: string): IconDefinition {
  return name.toLowerCase().includes("sync") ? faArrowsRotate : faLayerGroup;
}

export function statusIcon(status: string): IconDefinition {
  switch (status) {
    case "downloading":
      return faArrowDown;
    case "queued":
      return faClock;
    case "completed":
      return faCircleCheck;
    case "paused":
      return faPause;
    case "stopped":
      return faStop;
    case "failed":
      return faCircleExclamation;
    default:
      return faCircle;
  }
}

export function fileIcon(filename: string): IconDefinition {
  const ext = filename.split(".").pop()?.toLowerCase() ?? "";
  if (VIDEO.has(ext)) return faVideo;
  if (AUDIO.has(ext)) return faMusic;
  if (DOCS.has(ext)) return faFileLines;
  if (ARCHIVES.has(ext)) return faFileZipper;
  if (APPS.has(ext)) return faCube;
  if (IMAGES.has(ext)) return faImage;
  return faFile;
}
