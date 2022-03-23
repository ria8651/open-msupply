import React, { FC } from 'react';
import SvgIcon, { SvgIconProps } from '@mui/material/SvgIcon';
import { styled } from '@mui/material/styles';
import { keyframes } from '@mui/styled-engine';

const spin = keyframes`
  from {transform:rotate(0deg);}
  to {transform:rotate(360deg);}
}`;

const otherSpin = keyframes`
  from {transform:rotate(360deg);}
  to {transform:rotate(0deg);}
}`;

const sizes = {
  large: { height: 60, width: 45 },
  medium: { height: 40, width: 30 },
};

type MSupplyGuyProps = SvgIconProps & {
  size?: 'large' | 'medium';
};

const SvgGuy: FC<SvgIconProps & { fill: string }> = props => {
  const { fill, ...svgProps } = props;
  return (
    <SvgIcon {...svgProps} viewBox="3 1 206 310">
      <g>
        <linearGradient id="linear-grad" x1="0.5" y1="0" x2="0.5" y2="1">
          <stop offset="0" stopColor="#fa7a0a" />
          <stop offset="1" stopColor="#e83b30" />
        </linearGradient>
        <path
          fill={fill}
          stroke="#fff"
          strokeWidth={2}
          d="M101.077,27.0254 C101.077,27.0254,98.3577,24.3318,95.9549,23.5028 C94.3966,22.9662,121.462,13.6851,128.977,2.26597 C129.598,1.31825,132.617,5.90721,132.242,10.8023 C131.92,14.9647,125.838,34.9236,125.838,34.9236 C125.838,34.9236,134.908,35.5635,143.874,33.109 C142.379,37.5913,130.001,47.729,130.001,47.729 C130.001,47.729,140.109,61.3325,140.728,62.2596 C141.347,63.1884,138.92,65.7667,137.686,65.7667 C136.447,65.7667,130.854,90.2388,130.854,91.1676 C130.854,92.0947,119.956,94.7194,119.646,95.6499 C119.337,96.577,131.76,99.3686,141.659,124.727 C141.659,124.727,135.164,142.355,122.482,146.067 C122.482,146.067,102.69,145.757,109.495,129.675 C109.495,129.675,78.2372,124.04,78.5588,125.533 C78.877,127.026,96.9147,132.898,102.252,134.391 C102.868,134.66,102.678,137.487,102.678,137.487 C102.678,137.487,82.9448,142.2,83.7859,142.929 C86.1354,144.959,103.287,142.377,103.85,142.718 C104.417,143.057,104.324,143.677,104.705,144.746 C105.088,145.812,94.3089,151.6,94.8868,153.282 C95.4664,154.966,106.87,148.062,107.694,148.051 C108.518,148.043,111.129,148.008,111.108,148.906 C111.094,149.806,106.414,157.231,107.267,158.084 C108.12,158.937,114.417,150.721,115.27,150.293 C116.125,149.868,118.189,149.813,119.433,150.508 C119.433,151.896,118.791,154.99,119.221,155.31 C121.034,154.457,121.782,152.962,123.275,149.12 C126.475,149.759,143.554,135.033,145.678,124.109 C146.115,123.932,149.105,136.739,149.105,136.739 C149.105,136.739,150.917,132.898,151.772,129.376 C152.115,127.964,177.399,130.303,182.037,130.303 C186.676,130.303,196.86,129.599,203.543,124.293 C204.308,123.682,205.643,123.242,208.161,124.921 C203.752,133.115,161.547,147.41,156.908,148.027 C153.339,151.81,158.589,156.218,157.957,158.53 C152.978,162.002,151.655,166.093,144.935,170.924 C142.625,180.585,138.002,189.62,139.053,190.879 C177.203,201.301,180.254,245.252,177.285,256.626 C176.186,258.766,173.792,259.389,172.875,259.848 C173.922,264.408,170.106,277.815,170.106,277.815 C170.106,277.815,192.031,269.817,207.218,277.805 C201.625,281.768,200.103,285.849,200.103,285.849 C200.103,285.849,188.594,284.399,180.082,286.283 C171.143,288.259,150.914,303.218,124.851,308.3 C120.618,307.829,119.959,305.947,120.43,305.005 C120.898,304.064,125.133,305.005,126.194,303.474 C127.861,301.149,122.591,298.89,121.18,300.208 C138.399,268.501,111.731,244.645,108.01,244.788 C105.563,244.884,108.077,246.688,107.948,247.808 C81.0064,271.04,80.9789,283.07,81.2902,284.306 C81.5981,285.545,27.4679,279.6,27.4679,279.6 C27.4679,279.6,25.9612,290.464,20.3196,294.845 C16.687,297.662,12.8875,303.5,13.0113,306.568 C13.0698,308.113,10.2525,308.957,10.2525,308.957 C9.82246,310.225,8.36906,310.462,7.14785,310.18 C10.7203,288.163,11.3825,284.306,11.6628,279.04 C11.8503,271.604,7.33532,257.679,6.67656,257.586 C6.01781,257.492,5.64114,259.75,5.64114,259.75 C5.64114,259.75,4.04153,260.03,3.66486,259.562 C5.17156,245.543,6.86406,234.91,7.05153,232.842 C30.9526,234.628,40.1701,237.075,50.5228,234.91 C52.6848,231.43,56.6116,211.937,56.6116,211.937 L59.3946,213.176 L62.1775,189.361 L51.357,179.154 C51.357,179.154,68.2921,194.436,97.8744,192.235 C126.583,190.102,143.021,173.879,141.313,159.579 C136.936,166.409,125.572,181.634,94.6494,185.959 C93.0739,186.151,94.13,187.888,92.7523,188.076 C80.265,189.784,55.3973,181.992,48.2627,173.282 C40.2045,163.447,38.2162,143.251,45.4798,129.059 C43.0167,128.309,43.2059,128.808,43.0081,126.584 C42.8052,124.358,56.2126,104.169,75.8911,101.732 C76.7442,101.629,77.1691,95.6499,77.1691,95.6499 C77.1691,95.6499,71.622,90.9543,69.9124,88.1782 C68.3162,85.5862,68.3128,77.1874,68.3128,77.1874 L65.43,79.7485 L60.0138,72.7739 L58.495,74.1964 C58.495,74.1964,46.6769,62.8272,46.4068,60.4037 C45.3662,57.6551,47.0742,36.3099,101.077,27.0254"
        />
        <path
          fill="#fff"
          d="M80.1412,63.2718 C80.1412,63.2718,98.2597,46.0598,102.79,43.9459 C107.319,41.832,111.545,41.2283,112.752,40.0208 C113.962,38.8134,111.242,33.0755,110.336,33.0755 C109.431,33.0755,100.071,39.7181,98.5607,43.0412 C97.0505,46.3608,78.6328,60.8552,79.2365,62.0644"
        />
        <path
          fill="#fff"
          d="M83.1612,97.3916 L107.319,104.638 L107.016,106.148 L81.35,101.922 Z"
        />
        <path
          fill="#fffffe"
          d="M138.419,199.455 C138.419,199.455,116.678,227.537,117.583,228.743 C118.491,229.95,123.022,223.91,124.227,223.007 C125.433,222.102,129.663,222.102,129.663,222.102 C129.663,222.102,139.625,209.116,140.533,209.417 C141.439,209.722,145.364,213.647,145.364,213.647 C145.364,213.647,134.494,223.007,135.098,224.216 C135.703,225.423,138.118,225.423,138.118,225.423 C138.118,225.423,148.686,218.478,149.594,219.08 C150.499,219.685,153.516,222.704,153.516,222.704 C153.516,222.704,145.665,229.348,145.665,230.554 C145.665,231.763,155.327,228.743,155.327,228.743 C155.327,228.743,158.951,233.272,158.046,233.879 C157.141,234.479,153.215,236.294,153.215,236.294 L154.424,238.105 L159.255,237.5 L159.857,240.821 L157.444,242.03 L157.444,244.445 L161.367,244.749 L161.668,248.975 L159.556,248.671 L159.556,251.089 L161.367,251.39 L160.765,255.616 L163.179,255.917 C163.179,255.917,163.786,237.5,161.367,228.44 C158.951,219.383,143.252,200.057,138.419,199.455"
        />
        <path
          fill="#000"
          stroke="#fff"
          strokeWidth={2}
          d="M162.933,118.165 C161.551,117.737,159.965,116.153,159.491,114.727 C159.233,113.953,159.231,113.757,159.264,95.4329 L159.297,76.9202 L159.704,76.0617 C160.245,74.9187,161.076,74.0563,162.208,73.4615 C163.093,72.996,163.19,72.9746,164.764,72.8956 C166.365,72.8153,166.408,72.8053,166.784,72.4293 C167.274,71.94,167.399,71.379,167.403,69.673 L167.406,68.3002 L179.666,68.3002 L191.926,68.3002 L191.931,69.6092 C191.939,71.3719,192.064,71.9456,192.547,72.429 C192.937,72.8185,192.966,72.8254,194.574,72.9052 C196.346,72.9931,196.875,73.1521,197.893,73.9019 C198.682,74.4836,199.55,75.6572,199.845,76.5416 C200.098,77.3013,200.101,77.5777,200.068,95.825 L200.035,114.338 L199.628,115.196 C199.091,116.331,198.257,117.199,197.124,117.805 L196.204,118.296 L179.858,118.32 C166.132,118.341,163.419,118.316,162.933,118.165 M182.731,106.78 C183.649,106.208,183.684,106.052,183.731,102.312 L183.774,98.971 L187.116,98.9282 C190.288,98.8876,190.48,98.8713,190.904,98.6069 C191.762,98.0726,191.851,97.764,191.905,95.1417 C191.96,92.5344,191.859,92.0214,191.166,91.357 C190.644,90.8571,190.111,90.788,186.731,90.7818 L183.772,90.7762 L183.73,87.4878 C183.691,84.3982,183.672,84.168,183.411,83.6803 C182.934,82.7891,182.475,82.6669,179.606,82.6669 C177.405,82.6669,177.135,82.6911,176.742,82.9223 C176.184,83.2509,175.847,83.709,175.702,84.3369 C175.638,84.6125,175.584,86.174,175.583,87.807 L175.579,90.7762 L172.61,90.7792 C170.977,90.7809,169.416,90.8345,169.14,90.8984 C168.512,91.0441,168.054,91.381,167.726,91.9389 C167.494,92.3315,167.47,92.6021,167.47,94.8027 C167.47,97.6717,167.592,98.1305,168.484,98.6075 C168.971,98.8685,169.202,98.8879,172.291,98.9269 L175.579,98.9683 L175.585,101.928 C175.591,105.219,175.665,105.836,176.106,106.316 C176.78,107.047,177.198,107.134,179.858,107.095 C182.094,107.061,182.319,107.036,182.731,106.78 M164.353,66.0273 C164.113,65.9078,163.797,65.6478,163.65,65.4496 C163.391,65.0997,163.383,64.9345,163.348,59.8051 C163.307,53.8953,163.311,53.8656,164.201,53.2675 L164.635,52.9757 L179.655,52.9757 L194.676,52.9757 L195.151,53.2986 C196.029,53.8951,196.012,53.7711,196.012,59.6799 L196.012,64.992 L195.38,65.6245 L194.747,66.2569 L179.768,66.2508 C165.926,66.2452,164.755,66.2282,164.353,66.0273"
        />
      </g>
    </SvgIcon>
  );
};

// note: the linearGradient was removed in order to get the fill to work
// when used as a loader component in chrome :shrug:
const UnstyledGuy: FC<MSupplyGuyProps> = (svgProps): JSX.Element => (
  <SvgGuy fill="#e95c30" {...svgProps} />
);

export const MSupplyGuyGradient: FC<MSupplyGuyProps> = (
  svgProps
): JSX.Element => <MSupplyGuy fill="url(#linear-grad)" {...svgProps} />;

export const MSupplyGuy = styled(UnstyledGuy)(({ theme, size }) => {
  const animationStyle = {
    transition: theme.transitions.create('width', {
      easing: theme.transitions.easing.sharp,
      duration: theme.transitions.duration.leavingScreen,
    }),
    '&:hover': {
      animation:
        size === 'large'
          ? `${spin} 1s infinite ease`
          : `${otherSpin} 1s infinite ease`,
    },
  };
  const sizeStyle = size ? sizes[size] : {};
  return { ...animationStyle, ...sizeStyle };
});
