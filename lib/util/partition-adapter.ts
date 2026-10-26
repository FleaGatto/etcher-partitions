/*
 * Copyright 2026 balena.io
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * This adapter lists individual partitions as flashable targets, next to the
 * whole drives listed by the sdk's BlockDeviceAdapter. Writing to a partition
 * writes the image at the start of that partition only; the partition table of
 * the parent drive and the other partitions are left alone.
 */

import { execFile } from 'child_process';
import type { Drive as DrivelistDrive, Mountpoint } from 'drivelist';
import debug from 'debug';
import * as sdk from 'etcher-sdk';
import { Adapter } from 'etcher-sdk/build/scanner/adapters';
import { platform } from 'process';
import { promisify } from 'util';

const log = debug('etcher:partition-adapter');

const execFileAsync = promisify(execFile);

const SCAN_INTERVAL = 2000;

const EXEC_OPTIONS = {
	maxBuffer: 10 * 1024 * 1024,
	windowsHide: true,
};

/**
 * A partition, shaped like a drivelist drive so that the rest of Etcher can
 * treat it as a target. `isPartition` tells the writer not to touch the
 * partition table of the parent drive.
 */
export type PartitionDrive = DrivelistDrive & {
	displayName: string;
	isPartition: true;
	partitionParent: string | null;
	partitionIndex: number | null;
	/** Drive letter on Windows, used to dismount the volume before writing */
	partitionDriveLetter?: string;
};

/** The partition fields any drive object may carry, as seen by the writer. */
export interface MaybePartition {
	isPartition?: boolean;
	partitionDriveLetter?: string;
}

export function isPartitionDrive(
	drive: MaybePartition | undefined | null,
): boolean {
	return Boolean(drive && drive.isPartition);
}

function partitionKey(drive: PartitionDrive): string {
	return [
		drive.device,
		drive.size,
		drive.description,
		drive.mountpoints
			.map((m) => m.path)
			.sort()
			.join(','),
	].join('|');
}

export class PartitionAdapter extends Adapter {
	private partitions = new Map<string, sdk.sourceDestination.BlockDevice>();
	private running = false;
	private ready = false;
	private warned = false;

	public start(): void {
		this.running = true;
		this.scanLoop();
	}

	public stop(): void {
		this.running = false;
		this.ready = false;
		this.partitions.clear();
	}

	private async scanLoop(): Promise<void> {
		while (this.running) {
			await this.scan();
			if (!this.ready) {
				this.ready = true;
				this.emit('ready');
			}
			await delay(SCAN_INTERVAL);
		}
	}

	private async scan(): Promise<void> {
		const drives = await this.listPartitions();
		if (!this.running) {
			// we may have been stopped while listing the partitions.
			return;
		}
		for (const key of Array.from(this.partitions.keys())) {
			if (!drives.has(key)) {
				this.emit('detach', this.partitions.get(key));
				this.partitions.delete(key);
			}
		}
		for (const [key, drive] of drives) {
			if (this.partitions.has(key)) {
				continue;
			}
			const blockDevice = new sdk.sourceDestination.BlockDevice({
				drive,
				unmountOnSuccess: true,
				write: false,
				direct: true,
			});
			this.partitions.set(key, blockDevice);
			this.emit('attach', blockDevice);
		}
	}

	private async listPartitions(): Promise<Map<string, PartitionDrive>> {
		const result = new Map<string, PartitionDrive>();
		let drives: PartitionDrive[] = [];
		try {
			if (platform === 'linux') {
				drives = await listLinuxPartitions();
			} else if (platform === 'darwin') {
				drives = await listDarwinPartitions();
			} else if (platform === 'win32') {
				drives = await listWindowsPartitions();
			}
		} catch (error) {
			// Never emit 'error' here: the scanner stops every adapter on error,
			// which would also stop the whole-drive scanning.
			if (!this.warned) {
				this.warned = true;
				console.warn('Could not list partitions:', error);
			}
			return result;
		}
		for (const drive of drives) {
			result.set(partitionKey(drive), drive);
		}
		log('found', result.size, 'partitions');
		return result;
	}
}

function delay(ms: number): Promise<void> {
	return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * ConvertTo-Json can unwrap a single-element collection into a bare object, so
 * never trust that a list from PowerShell is an array.
 */
function asArray<T>(value: T[] | T | null | undefined): T[] {
	if (value === null || value === undefined) {
		return [];
	}
	return Array.isArray(value) ? value : [value];
}

function mountpoint(path: string, label: string | null): Mountpoint {
	return { path, label };
}

/**
 * What the target list shows in its name column. The device path or drive
 * letter is shown next to it, in the location column.
 */
function partitionDescription(
	index: number | null,
	...parts: Array<string | null | undefined>
): string {
	return [index === null ? 'Partition' : `Partition ${index}`, ...parts]
		.map((part) => (part || '').trim())
		.filter((part) => part !== '')
		.join(' · ');
}

// ---------------------------------------------------------------- linux

interface LsblkDevice {
	name: string;
	kname?: string;
	path?: string;
	size?: number | string | null;
	type?: string;
	rm?: boolean | null;
	ro?: boolean | null;
	mountpoints?: Array<string | null>;
	mountpoint?: string | null;
	label?: string | null;
	partlabel?: string | null;
	fstype?: string | null;
	pkname?: string | null;
	model?: string | null;
	vendor?: string | null;
	tran?: string | null;
	pttype?: string | null;
	'phy-sec'?: number | null;
	'log-sec'?: number | null;
	children?: LsblkDevice[];
}

const LSBLK_COLUMNS = [
	'NAME',
	'KNAME',
	'PATH',
	'SIZE',
	'TYPE',
	'RM',
	'RO',
	'MOUNTPOINTS',
	'LABEL',
	'PARTLABEL',
	'FSTYPE',
	'PKNAME',
	'MODEL',
	'VENDOR',
	'TRAN',
	'PTTYPE',
	'PHY-SEC',
	'LOG-SEC',
];

async function runLsblk(): Promise<LsblkDevice[]> {
	let stdout: string;
	try {
		({ stdout } = await execFileAsync(
			'lsblk',
			['-J', '-b', '-p', '-o', LSBLK_COLUMNS.join(',')],
			EXEC_OPTIONS,
		));
	} catch (error) {
		// lsblk older than 2.37 doesn't know about the MOUNTPOINTS column.
		({ stdout } = await execFileAsync(
			'lsblk',
			[
				'-J',
				'-b',
				'-p',
				'-o',
				LSBLK_COLUMNS.filter((c) => c !== 'MOUNTPOINTS')
					.concat('MOUNTPOINT')
					.join(','),
			],
			EXEC_OPTIONS,
		));
	}
	return JSON.parse(stdout).blockdevices || [];
}

function lsblkMountpoints(device: LsblkDevice): string[] {
	const paths = device.mountpoints ?? [device.mountpoint ?? null];
	return paths.filter((path): path is string => Boolean(path));
}

/**
 * Mountpoints we don't want anyone to flash over by accident. Partitions of a
 * drive holding any of those are reported as system partitions, which hides
 * them behind Etcher's "show hidden" toggle and shows a warning when selected.
 */
const LINUX_SYSTEM_MOUNTPOINTS = [
	'/',
	'/boot',
	'/efi',
	'/etc',
	'/sysroot',
	'/usr',
	'/var',
];

function isLinuxSystemMountpoint(path: string): boolean {
	if (path === '[SWAP]') {
		return true;
	}
	return LINUX_SYSTEM_MOUNTPOINTS.some(
		(systemPath) =>
			path === systemPath ||
			(systemPath === '/' ? false : path.startsWith(`${systemPath}/`)),
	);
}

function hasSystemMountpoint(device: LsblkDevice): boolean {
	if (lsblkMountpoints(device).some(isLinuxSystemMountpoint)) {
		return true;
	}
	return (device.children || []).some(hasSystemMountpoint);
}

function toNumber(value: number | string | null | undefined): number | null {
	if (typeof value === 'number') {
		return value;
	}
	if (typeof value === 'string' && value !== '') {
		const parsed = Number(value);
		return Number.isNaN(parsed) ? null : parsed;
	}
	return null;
}

function partitionIndexOf(name: string): number | null {
	const match = /(\d+)$/.exec(name);
	return match === null ? null : Number(match[1]);
}

async function listLinuxPartitions(): Promise<PartitionDrive[]> {
	const disks = await runLsblk();
	const partitions: PartitionDrive[] = [];

	const walk = (device: LsblkDevice, disk: LsblkDevice, isSystem: boolean) => {
		for (const child of device.children || []) {
			if (child.type === 'part') {
				const size = toNumber(child.size);
				if (size !== null && size > 0) {
					partitions.push(linuxPartition(child, disk, isSystem));
				}
			}
			walk(child, disk, isSystem);
		}
	};

	for (const disk of disks) {
		if (disk.type !== 'disk') {
			continue;
		}
		// Loop and zram devices are not useful targets.
		if ((disk.path ?? '').startsWith('/dev/zram')) {
			continue;
		}
		walk(disk, disk, hasSystemMountpoint(disk));
	}

	return partitions;
}

function linuxPartition(
	partition: LsblkDevice,
	disk: LsblkDevice,
	diskIsSystem: boolean,
): PartitionDrive {
	const path = partition.path ?? `/dev/${partition.name}`;
	const mountpoints = lsblkMountpoints(partition);
	const name =
		(partition.label || partition.partlabel || partition.fstype || '').trim() ||
		'Partition';
	const index = partitionIndexOf(partition.name);

	return {
		enumerator: 'lsblk',
		busType: (disk.tran || 'UNKNOWN').toUpperCase(),
		busVersion: null,
		device: path,
		devicePath: null,
		raw: path,
		description: partitionDescription(index, name),
		error: null,
		size: toNumber(partition.size),
		blockSize: toNumber(partition['phy-sec']) || 512,
		logicalBlockSize: toNumber(partition['log-sec']) || 512,
		mountpoints: mountpoints
			.filter((mp) => mp !== '[SWAP]')
			.map((mp) => mountpoint(mp, partition.label ?? null)),
		isReadOnly: Boolean(partition.ro),
		isSystem: diskIsSystem || mountpoints.some(isLinuxSystemMountpoint),
		isVirtual: false,
		isRemovable: Boolean(partition.rm ?? disk.rm),
		isCard: null,
		isSCSI: null,
		isUAS: null,
		isUSB: disk.tran === 'usb',
		partitionTableType:
			disk.pttype === 'gpt' ? 'gpt' : disk.pttype === 'dos' ? 'mbr' : null,
		displayName: path,
		isPartition: true,
		partitionParent: disk.path ?? null,
		partitionIndex: index,
	};
}

// ---------------------------------------------------------------- macOS

interface DiskutilPartition {
	DeviceIdentifier: string;
	Size?: number;
	Content?: string;
	VolumeName?: string | null;
	MountPoint?: string | null;
}

interface DiskutilDisk {
	DeviceIdentifier: string;
	Size?: number;
	Content?: string;
	Partitions?: DiskutilPartition[];
	APFSVolumes?: DiskutilPartition[];
}

interface DiskutilInfo {
	Internal?: boolean;
	Removable?: boolean;
	RemovableMedia?: boolean;
	BusProtocol?: string;
	MediaName?: string;
	DeviceBlockSize?: number;
	Content?: string;
	WritableMedia?: boolean;
}

const darwinDiskInfoCache = new Map<string, DiskutilInfo>();

async function plist<T>(command: string): Promise<T> {
	const { stdout } = await execFileAsync(
		'/bin/sh',
		['-c', `${command} | plutil -convert json -o - -`],
		EXEC_OPTIONS,
	);
	return JSON.parse(stdout);
}

async function darwinDiskInfo(identifier: string): Promise<DiskutilInfo> {
	let info = darwinDiskInfoCache.get(identifier);
	if (info === undefined) {
		info = await plist<DiskutilInfo>(`diskutil info -plist ${identifier}`);
		darwinDiskInfoCache.set(identifier, info);
	}
	return info;
}

const DARWIN_SYSTEM_MOUNTPOINT_PREFIXES = ['/System', '/private', '/usr'];

function isDarwinSystemMountpoint(path: string): boolean {
	return (
		path === '/' ||
		DARWIN_SYSTEM_MOUNTPOINT_PREFIXES.some((prefix) => path.startsWith(prefix))
	);
}

async function listDarwinPartitions(): Promise<PartitionDrive[]> {
	const { AllDisksAndPartitions: disks } = await plist<{
		AllDisksAndPartitions: DiskutilDisk[];
	}>('diskutil list -plist');
	const partitions: PartitionDrive[] = [];

	for (const disk of asArray(disks)) {
		const info = await darwinDiskInfo(disk.DeviceIdentifier);
		const diskIsSystem = Boolean(info.Internal);
		const children = [...(disk.Partitions || []), ...(disk.APFSVolumes || [])];
		for (const partition of children) {
			if (!partition.Size) {
				continue;
			}
			const identifier = partition.DeviceIdentifier;
			const mountPoint = partition.MountPoint || null;
			const name =
				(partition.VolumeName || partition.Content || '').trim() || 'Partition';
			partitions.push({
				enumerator: 'diskutil',
				busType: (info.BusProtocol || 'UNKNOWN').toUpperCase(),
				busVersion: null,
				device: `/dev/${identifier}`,
				devicePath: null,
				raw: `/dev/r${identifier}`,
				description: partitionDescription(
					partitionIndexOf(identifier),
					name,
					info.MediaName,
				),
				error: null,
				size: partition.Size,
				blockSize: info.DeviceBlockSize || 512,
				logicalBlockSize: info.DeviceBlockSize || 512,
				mountpoints: mountPoint
					? [mountpoint(mountPoint, partition.VolumeName ?? null)]
					: [],
				isReadOnly: info.WritableMedia === false,
				isSystem:
					diskIsSystem ||
					(mountPoint !== null && isDarwinSystemMountpoint(mountPoint)),
				isVirtual: false,
				isRemovable: Boolean(info.Removable ?? info.RemovableMedia),
				isCard: null,
				isSCSI: null,
				isUAS: null,
				isUSB: (info.BusProtocol || '').toUpperCase() === 'USB',
				partitionTableType:
					disk.Content === 'GUID_partition_scheme' ? 'gpt' : null,
				displayName: `/dev/${identifier}`,
				isPartition: true,
				partitionParent: `/dev/${disk.DeviceIdentifier}`,
				partitionIndex: partitionIndexOf(identifier),
			});
		}
	}

	return partitions;
}

// ---------------------------------------------------------------- windows

interface WindowsDisk {
	DiskNumber: number;
	FriendlyName?: string | null;
	BusType?: string | null;
	IsSystem?: boolean;
	IsBoot?: boolean;
	IsOffline?: boolean;
	PartitionStyle?: string | null;
	PhysicalSectorSize?: number | null;
	LogicalSectorSize?: number | null;
}

interface WindowsPartition {
	DiskNumber: number;
	PartitionNumber: number;
	DriveLetter?: string | null;
	Size?: number | null;
	IsBoot?: boolean;
	IsSystem?: boolean;
	IsHidden?: boolean;
	Type?: string | null;
	AccessPaths?: string[] | null;
}

interface WindowsVolume {
	DriveLetter?: string | null;
	FileSystemLabel?: string | null;
	FileSystem?: string | null;
	DriveType?: string | null;
}

const POWERSHELL_LIST_PARTITIONS = `
$ErrorActionPreference = 'Stop'
$disks = @(Get-Disk | Select-Object FriendlyName, IsSystem, IsBoot, IsOffline, PhysicalSectorSize, LogicalSectorSize,
	@{ n = 'DiskNumber'; e = { $_.Number } },
	@{ n = 'BusType'; e = { [string] $_.BusType } },
	@{ n = 'PartitionStyle'; e = { [string] $_.PartitionStyle } })
$partitions = @(Get-Partition | Select-Object DiskNumber, PartitionNumber, Size, IsBoot, IsSystem, IsHidden,
	@{ n = 'DriveLetter'; e = { [string] $_.DriveLetter } },
	@{ n = 'Type'; e = { [string] $_.Type } },
	@{ n = 'AccessPaths'; e = { @($_.AccessPaths) } })
$volumes = @(Get-Volume | Select-Object FileSystemLabel,
	@{ n = 'DriveLetter'; e = { [string] $_.DriveLetter } },
	@{ n = 'FileSystem'; e = { [string] $_.FileSystem } },
	@{ n = 'DriveType'; e = { [string] $_.DriveType } })
ConvertTo-Json -Depth 4 -Compress -InputObject @{ disks = $disks; partitions = $partitions; volumes = $volumes }
`;

async function powershell<T>(script: string): Promise<T> {
	const { stdout } = await execFileAsync(
		'powershell.exe',
		[
			'-NoProfile',
			'-NonInteractive',
			'-ExecutionPolicy',
			'Bypass',
			'-Command',
			script,
		],
		EXEC_OPTIONS,
	);
	return JSON.parse(stdout);
}

/**
 * A volume GUID path keeps working after the volume has been taken offline and
 * lost its drive letter, so it is preferred over `\\.\X:` as the raw path.
 */
function windowsRawPath(partition: WindowsPartition): string | null {
	for (const accessPath of asArray(partition.AccessPaths)) {
		if (accessPath.startsWith('\\\\?\\Volume{')) {
			return accessPath.replace(/\\+$/, '');
		}
	}
	if (partition.DriveLetter) {
		return `\\\\.\\${partition.DriveLetter}:`;
	}
	return null;
}

async function listWindowsPartitions(): Promise<PartitionDrive[]> {
	const { disks, partitions, volumes } = await powershell<{
		disks: WindowsDisk[];
		partitions: WindowsPartition[];
		volumes: WindowsVolume[];
	}>(POWERSHELL_LIST_PARTITIONS);

	const disksByNumber = new Map(asArray(disks).map((d) => [d.DiskNumber, d]));
	const volumesByLetter = new Map(
		asArray(volumes)
			.filter((v) => Boolean(v.DriveLetter))
			.map((v) => [v.DriveLetter as string, v]),
	);
	const result: PartitionDrive[] = [];

	for (const partition of asArray(partitions)) {
		const raw = windowsRawPath(partition);
		if (raw === null || !partition.Size) {
			// Nothing we can open: no drive letter and no volume GUID path.
			continue;
		}
		const disk = disksByNumber.get(partition.DiskNumber);
		const volume = partition.DriveLetter
			? volumesByLetter.get(partition.DriveLetter)
			: undefined;
		const letter = partition.DriveLetter || null;
		const name =
			(volume?.FileSystemLabel || partition.Type || '').trim() || 'Partition';
		// The target list keys its rows by displayName, and the sdk names a whole
		// drive after its mountpoints on Windows, so a drive whose only mounted
		// partition is E: is also called `E:\`. Keep the partition's name
		// distinct from it: colliding names make the two rows share a key, and a
		// disabled drive then greys out the partition as well.
		const displayName = letter
			? `${letter}:\\ (partition ${partition.PartitionNumber})`
			: `Disk ${partition.DiskNumber} partition ${partition.PartitionNumber}`;

		result.push({
			enumerator: 'Get-Partition',
			busType: (disk?.BusType || 'UNKNOWN').toUpperCase(),
			busVersion: null,
			device: raw,
			devicePath: null,
			raw,
			description: partitionDescription(
				partition.PartitionNumber,
				name,
				disk?.FriendlyName,
			),
			error: null,
			size: partition.Size,
			blockSize: disk?.PhysicalSectorSize || 512,
			logicalBlockSize: disk?.LogicalSectorSize || 512,
			mountpoints: letter
				? [mountpoint(`${letter}:\\`, volume?.FileSystemLabel ?? null)]
				: [],
			isReadOnly: false,
			isSystem: Boolean(
				partition.IsSystem ||
					partition.IsBoot ||
					disk?.IsSystem ||
					disk?.IsBoot ||
					letter === 'C',
			),
			isVirtual: false,
			isRemovable: (volume?.DriveType || '').toUpperCase() === 'REMOVABLE',
			isCard: null,
			isSCSI: null,
			isUAS: null,
			isUSB: (disk?.BusType || '').toUpperCase() === 'USB',
			partitionTableType:
				disk?.PartitionStyle === 'GPT'
					? 'gpt'
					: disk?.PartitionStyle === 'MBR'
						? 'mbr'
						: null,
			displayName,
			isPartition: true,
			partitionParent: `\\\\.\\PhysicalDrive${partition.DiskNumber}`,
			partitionIndex: partition.PartitionNumber,
			partitionDriveLetter: letter ?? undefined,
		});
	}

	return result;
}

/**
 * Windows refuses raw writes to a volume that is still mounted, so take it
 * offline first. Best effort: if diskpart fails the write will fail with a
 * clear EBUSY/EPERM error anyway. No-op on every other platform, where the sdk
 * unmounts the device itself before opening it.
 */
export async function dismountPartition(drive: MaybePartition): Promise<void> {
	if (platform !== 'win32' || !isPartitionDrive(drive)) {
		return;
	}
	const letter = drive.partitionDriveLetter;
	if (!letter) {
		// No access path, so nothing has it mounted.
		return;
	}
	const script = [
		`select volume ${letter}`,
		'offline volume noerr',
		'rescan',
		'exit',
	].join('\r\n');
	try {
		await execFileAsync(
			'cmd.exe',
			['/c', `echo ${script.split('\r\n').join(' & echo ')} | diskpart`],
			EXEC_OPTIONS,
		);
		log('took volume', letter, 'offline');
	} catch (error) {
		console.warn(`Could not take volume ${letter}: offline:`, error);
	}
}
